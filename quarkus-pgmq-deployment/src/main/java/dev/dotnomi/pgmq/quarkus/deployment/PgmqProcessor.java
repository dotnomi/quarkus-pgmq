package dev.dotnomi.pgmq.quarkus.deployment;

import dev.dotnomi.pgmq.quarkus.PgmqLifecycle;
import dev.dotnomi.pgmq.quarkus.PgmqProducers;
import dev.dotnomi.pgmq.quarkus.PgmqRecorder;
import dev.dotnomi.pgmq.quarkus.RecordedListener;
import io.quarkus.agroal.spi.JdbcDataSourceBuildItem;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.BeanArchiveIndexBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageProxyDefinitionBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Build-time augmentation for the PGMQ extension.
 *
 * Besides the wiring, this validates listener signatures and per-queue settings, so mistakes surface
 * as build failures rather than as puzzling runtime behaviour.
 */
class PgmqProcessor {
    private static final String FEATURE = "pgmq";
    private static final DotName LISTENER = DotName.createSimple("dev.dotnomi.pgmq.quarkus.PgmqListener");
    private static final DotName TOPIC_LISTENER =
            DotName.createSimple("dev.dotnomi.pgmq.quarkus.PgmqTopicListener");
    private static final DotName PUBLISHER = DotName.createSimple("dev.dotnomi.pgmq.quarkus.PgmqPublisher");
    private static final DotName CUSTOMIZER =
            DotName.createSimple("dev.dotnomi.pgmq.quarkus.PgmqMessageCustomizer");
    private static final DotName PGMQ_MESSAGE = DotName.createSimple("dev.dotnomi.pgmq.PgmqMessage");
    private static final DotName PGMQ_CONTEXT = DotName.createSimple("dev.dotnomi.pgmq.listener.PgmqContext");
    private static final DotName LIST = DotName.createSimple("java.util.List");

    /** Per-queue settings: if two handlers on the same queue disagree, that is a build error. */
    private static final List<String> PER_QUEUE_ATTRIBUTES = List.of(
        "concurrency",
        "batchSize",
        "pollInterval",
        "messageInterval",
        "visibilityTimeout",
        "vtRefresh",
        "ackMode",
        "maxRetries",
        "deadLetterQueue",
        "autoStart",
        "fifo",
        "envelopeValidation",
        "unroutable"
    );

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    /**
     * Initialises the UUID generator at run time.
     *
     * A native image would otherwise bake its {@code SecureRandom} seed into the image heap, and
     * every instance would produce identical {@code messageId} values.
     */
    @BuildStep
    RuntimeInitializedClassBuildItem uuidGeneratorAtRuntime() {
        return new RuntimeInitializedClassBuildItem("dev.dotnomi.pgmq.UuidV7");
    }

    @BuildStep
    AdditionalBeanBuildItem beans() {
        return AdditionalBeanBuildItem.builder()
            .addBeanClasses(
                dev.dotnomi.pgmq.quarkus.PgmqRuntimeConfig.class,
                dev.dotnomi.pgmq.quarkus.PgmqTemplateRegistry.class,
                dev.dotnomi.pgmq.quarkus.RecordedListeners.class,
                dev.dotnomi.pgmq.quarkus.PgmqOwnedSubscriptions.class,
                dev.dotnomi.pgmq.quarkus.PgmqTopicMaintenance.class,
                dev.dotnomi.pgmq.quarkus.PgmqMetricsHolder.class,
                dev.dotnomi.pgmq.quarkus.PgmqPublishInvoker.class,
                dev.dotnomi.pgmq.quarkus.PgmqPublisherInterceptor.class,
                PgmqProducers.class,
                PgmqLifecycle.class
            )
            .setUnremovable()
            .build();
    }

    /**
     * Metrics beans, only when the application actually brought Micrometer along.
     *
     * Registered by name so that neither class is loaded at build time — their signatures reference
     * MeterRegistry, which need not be on the classpath at all.
     */
    @BuildStep
    void metrics(Capabilities capabilities, BuildProducer<AdditionalBeanBuildItem> beans) {
        if (!capabilities.isPresent(Capability.METRICS)) {
            return;
        }
        beans.produce(AdditionalBeanBuildItem.builder()
            .addBeanClasses(
                "dev.dotnomi.pgmq.quarkus.PgmqMicrometerMetrics",
                "dev.dotnomi.pgmq.quarkus.PgmqQueueGauges"
            )
            .setUnremovable()
            .build());
    }

    @BuildStep
    void collectListeners(
        BeanArchiveIndexBuildItem beanArchiveIndex,
        CombinedIndexBuildItem combinedIndex,
        BuildProducer<ListenerMethodBuildItem> listeners,
        BuildProducer<ReflectiveClassBuildItem> reflective,
        BuildProducer<UnremovableBeanBuildItem> unremovable
    ) {
        IndexView index = beanArchiveIndex.getIndex();
        List<String> errors = new ArrayList<>();
        List<RecordedListener> collected = new ArrayList<>();
        Set<String> reflectiveClasses = new LinkedHashSet<>();
        Set<String> listenerBeans = new LinkedHashSet<>();

        for (AnnotationInstance annotation : index.getAnnotations(LISTENER)) {
            if (annotation.target().kind() != AnnotationTarget.Kind.METHOD) {
                continue;
            }
            MethodInfo method = annotation.target().asMethod();
            String where = method.declaringClass().name() + "#" + method.name();

            Signature signature;
            try {
                signature = resolveSignature(method);
            } catch (IllegalArgumentException exception) {
                errors.add(where + ": " + exception.getMessage());
                continue;
            }

            String rawError = validateRaw(annotation, signature);
            if (rawError != null) {
                errors.add(where + ": " + rawError);
                continue;
            }

            collected.add(toRecorded(annotation, method, signature));
            reflectiveClasses.add(method.declaringClass().name().toString());
            reflectiveClasses.add(signature.payloadClassName());
            listenerBeans.add(method.declaringClass().name().toString());
        }

        for (AnnotationInstance annotation : index.getAnnotations(TOPIC_LISTENER)) {
            if (annotation.target().kind() != AnnotationTarget.Kind.METHOD) {
                continue;
            }
            MethodInfo method = annotation.target().asMethod();
            String where = method.declaringClass().name() + "#" + method.name();

            Signature signature;
            try {
                signature = resolveSignature(method);
            } catch (IllegalArgumentException exception) {
                errors.add(where + ": " + exception.getMessage());
                continue;
            }

            String rawError = validateRaw(annotation, signature);
            if (rawError != null) {
                errors.add(where + ": " + rawError);
                continue;
            }

            collected.add(toRecordedTopic(annotation, method, signature));
            reflectiveClasses.add(method.declaringClass().name().toString());
            reflectiveClasses.add(signature.payloadClassName());
            listenerBeans.add(method.declaringClass().name().toString());
        }

        // Jackson serialises publisher payloads reflectively, so they must survive native image.
        for (AnnotationInstance annotation : index.getAnnotations(PUBLISHER)) {
            if (annotation.target().kind() != AnnotationTarget.Kind.METHOD) {
                continue;
            }
            MethodInfo method = annotation.target().asMethod();
            for (Type parameter : method.parameterTypes()) {
                if (CUSTOMIZER.equals(parameter.name())) {
                    continue;
                }
                reflectiveClasses.add(parameter.name().toString());
            }
        }

        errors.addAll(validateQueueConsistency(index));

        if (!errors.isEmpty()) {
            throw new IllegalStateException("Invalid @PgmqListener configuration:\n  - " + String.join("\n  - ", errors));
        }

        collected.forEach(listener -> listeners.produce(new ListenerMethodBuildItem(listener)));

        // ArC would otherwise remove the bean as unused: the reflective call is invisible to it.
        if (!listenerBeans.isEmpty()) {
            unremovable.produce(UnremovableBeanBuildItem.beanClassNames(listenerBeans.toArray(new String[0])));
        }

        // Handler invocation and payload deserialization are reflective.
        if (!reflectiveClasses.isEmpty()) {
            reflective.produce(
                ReflectiveClassBuildItem
                    .builder(reflectiveClasses.toArray(new String[0]))
                    .methods()
                    .fields()
                    .build()
            );
        }
    }

    /**
     * Supplies an implementation for every {@code @PgmqPublisher} declared as an interface.
     *
     * An interface cannot be a CDI bean, so the interceptor route does not apply; a dynamic proxy is
     * registered as a synthetic bean instead. Abstract methods publish, {@code default} methods keep
     * their own body.
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void publisherInterfaces(PgmqRecorder recorder,
                             BeanArchiveIndexBuildItem beanArchiveIndex,
                             BuildProducer<SyntheticBeanBuildItem> syntheticBeans,
                             BuildProducer<NativeImageProxyDefinitionBuildItem> proxies) {

        List<String> errors = new ArrayList<>();
        for (AnnotationInstance annotation : beanArchiveIndex.getIndex().getAnnotations(PUBLISHER)) {
            if (annotation.target().kind() != AnnotationTarget.Kind.CLASS) {
                continue;
            }
            ClassInfo declaring = annotation.target().asClass();
            if (!Modifier.isInterface(declaring.flags())) {
                // An abstract class is neither instantiable by CDI nor a valid proxy target, so it
                // would otherwise fail as an unsatisfied injection point with no hint at the cause.
                if (Modifier.isAbstract(declaring.flags())) {
                    errors.add(declaring.name() + " is abstract — declare it as an interface, or as a "
                            + "concrete class whose method bodies are ignored");
                }
                continue;
            }

            String name = declaring.name().toString();
            syntheticBeans.produce(SyntheticBeanBuildItem
                    .configure(declaring.name())
                    .scope(jakarta.inject.Singleton.class)
                    .unremovable()
                    .setRuntimeInit()
                    .createWith(recorder.publisherProxy(name))
                    .done());

            // JDK dynamic proxies must be declared for native image.
            proxies.produce(new NativeImageProxyDefinitionBuildItem(name));
        }

        if (!errors.isEmpty()) {
            throw new IllegalStateException("Invalid @PgmqPublisher declaration:\n  - "
                    + String.join("\n  - ", errors));
        }
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void registerListeners(
        PgmqRecorder recorder,
        List<ListenerMethodBuildItem> listeners,
        // Ordering only: containers must not be built before the datasource exists.
        List<JdbcDataSourceBuildItem> dataSources
    ) {
        if (listeners.isEmpty()) {
            return;
        }
        recorder.registerListeners(listeners.stream().map(ListenerMethodBuildItem::listener).collect(Collectors.toList()));
    }

    // ----------------------------------------------------------------------------------------
    // Signature analysis
    // ----------------------------------------------------------------------------------------

    private record Signature(String kind, String payloadClassName) { }

    /** Determines how the handler wants its arguments and which type the payload becomes. */
    private Signature resolveSignature(MethodInfo method) {
        List<Type> parameters = method.parameterTypes();

        if (parameters.isEmpty() || parameters.size() > 2) {
            throw new IllegalArgumentException(unsupportedSignature(method));
        }

        Type first = parameters.get(0);

        if (parameters.size() == 2) {
            if (!PGMQ_CONTEXT.equals(parameters.get(1).name())) {
                throw new IllegalArgumentException(unsupportedSignature(method));
            }
            if (PGMQ_MESSAGE.equals(first.name())) {
                return new Signature(RecordedListener.SIGNATURE_MESSAGE_CONTEXT, unwrapMessage(first, method));
            }
            return new Signature(RecordedListener.SIGNATURE_PAYLOAD_CONTEXT, first.name().toString());
        }

        if (LIST.equals(first.name())) {
            Type element = singleArgument(first, method);
            if (!PGMQ_MESSAGE.equals(element.name())) {
                throw new IllegalArgumentException("a batch handler must take List<PgmqMessage<T>>, not List<" + element.name() + ">");
            }
            return new Signature(RecordedListener.SIGNATURE_BATCH, unwrapMessage(element, method));
        }

        if (PGMQ_MESSAGE.equals(first.name())) {
            return new Signature(RecordedListener.SIGNATURE_MESSAGE, unwrapMessage(first, method));
        }

        return new Signature(RecordedListener.SIGNATURE_PAYLOAD, first.name().toString());
    }

    private String unwrapMessage(Type messageType, MethodInfo method) {
        return singleArgument(messageType, method).name().toString();
    }

    private Type singleArgument(Type type, MethodInfo method) {
        if (type.kind() != Type.Kind.PARAMETERIZED_TYPE) {
            throw new IllegalArgumentException(
                type.name() + " must carry an explicit type argument so the payload type is known "
                + "at build time — a raw type cannot be deserialised into anything specific"
            );
        }
        ParameterizedType parameterized = type.asParameterizedType();
        if (parameterized.arguments().size() != 1) {
            throw new IllegalArgumentException(unsupportedSignature(method));
        }
        return parameterized.arguments().get(0);
    }

    /** {@code raw = "true"} skips deserialization, so the payload parameter has to be a String. */
    private String validateRaw(AnnotationInstance annotation, Signature signature) {
        if (!"true".equals(stringValue(annotation, "raw", "false"))) {
            return null;
        }
        if (!String.class.getName().equals(signature.payloadClassName())) {
            return "raw = \"true\" hands over the JSON text unparsed, so the payload must be String "
                    + "but is " + signature.payloadClassName();
        }
        return null;
    }

    private String unsupportedSignature(MethodInfo method) {
        return "unsupported signature " + method.parameterTypes() + ". A @PgmqListener method must "
            + "take one of: (T payload), (PgmqMessage<T> message), (T payload, PgmqContext context), "
            + "(PgmqMessage<T> message, PgmqContext context), or (List<PgmqMessage<T>> batch)";
    }

    // ----------------------------------------------------------------------------------------
    // Cross-handler validation
    // ----------------------------------------------------------------------------------------

    /** All handlers of a queue share one container, so their per-queue settings must agree. */
    private List<String> validateQueueConsistency(IndexView index) {
        Map<String, List<AnnotationInstance>> byQueue = new HashMap<>();

        for (AnnotationInstance annotation : index.getAnnotations(LISTENER)) {
            if (annotation.target().kind() != AnnotationTarget.Kind.METHOD) {
                continue;
            }
            String key = stringValue(annotation, "client", "") + "/" + stringValue(annotation, "queue", "");
            byQueue.computeIfAbsent(key, k -> new ArrayList<>()).add(annotation);
        }

        List<String> errors = new ArrayList<>();

        byQueue.forEach((queue, annotations) -> {
            List<String> catchAlls = annotations.stream()
                .filter(a -> stringValue(a, "label", "").isBlank())
                .map(this::describe)
                .collect(Collectors.toList());

            if (catchAlls.size() > 1) {
                errors.add(
                    "queue '" + queue.substring(queue.indexOf('/') + 1) + "' has "
                    + catchAlls.size() + " catch-all handlers (" + String.join(", ", catchAlls)
                    + "), but at most one is allowed. A handler without a label picks up "
                    + "everything no labelled handler claims."
                );
            }

            Map<String, List<String>> duplicateLabels = annotations.stream()
                .filter(a -> !stringValue(a, "label", "").isBlank())
                .collect(
                    Collectors.groupingBy(
                        a -> stringValue(a, "label", ""),
                        Collectors.mapping(this::describe, Collectors.toList())
                    )
                );
            duplicateLabels.forEach((label, methods) -> {
                if (methods.size() > 1) {
                    errors.add(
                        "queue '" + queue.substring(queue.indexOf('/') + 1) + "' has several "
                        + "handlers for label '" + label + "': " + String.join(", ", methods)
                    );
                }
            });

            for (String attribute : PER_QUEUE_ATTRIBUTES) {
                Map<String, List<String>> distinct = annotations.stream()
                    .collect(
                        Collectors.groupingBy(
                            a -> attributeAsString(a, attribute),
                            Collectors.mapping(this::describe, Collectors.toList())
                        )
                    );
                if (distinct.size() > 1) {
                    errors.add(
                        "queue '" + queue.substring(queue.indexOf('/') + 1) + "' has "
                        + "conflicting values for '" + attribute + "': " + distinct
                        + ". This setting applies per queue, because every handler on a queue "
                        + "shares one container — it must match across all of them."
                    );
                }
            }
        });

        return errors;
    }

    private String describe(AnnotationInstance annotation) {
        MethodInfo method = annotation.target().asMethod();
        return method.declaringClass().simpleName() + "#" + method.name();
    }

    private String attributeAsString(AnnotationInstance annotation, String name) {
        AnnotationValue value = annotation.value(name);
        return value == null ? "<default>" : value.asString();
    }

    private static String stringValue(AnnotationInstance annotation, String name, String fallback) {
        AnnotationValue value = annotation.value(name);
        return value == null ? fallback : value.asString();
    }

    private static String enumValue(AnnotationInstance annotation, String name, String fallback) {
        AnnotationValue value = annotation.value(name);
        return value == null ? fallback : value.asEnum();
    }

    /** Same settings as a plain listener; the queue is only known once the subscription exists. */
    private RecordedListener toRecordedTopic(AnnotationInstance a, MethodInfo method, Signature signature) {
        return new RecordedListener(
            method.declaringClass().name().toString(),
            method.name(),
            signature.payloadClassName(),
            signature.kind(),
            "",
            stringValue(a, "label", ""),
            stringValue(a, "client", ""),
            stringValue(a, "concurrency", "1"),
            stringValue(a, "batchSize", "1"),
            stringValue(a, "pollInterval", "5s"),
            stringValue(a, "messageInterval", "0s"),
            stringValue(a, "visibilityTimeout", ""),
            stringValue(a, "vtRefresh", "false"),
            enumValue(a, "ackMode", "DELETE"),
            stringValue(a, "maxRetries", "5"),
            stringValue(a, "deadLetterQueue", ""),
            "true",
            stringValue(a, "autoStart", "true"),
            stringValue(a, "fifo", "false"),
            stringValue(a, "schemaVersions", ""),
            enumValue(a, "envelopeValidation", "STRICT"),
            enumValue(a, "unroutable", "DLQ"),
            stringValue(a, "batch", "false"),
            stringValue(a, "topic", ""),
            stringValue(a, "group", ""),
            enumValue(a, "mode", "SHARED"),
            stringValue(a, "raw", "false")
        );
    }

    private RecordedListener toRecorded(AnnotationInstance a, MethodInfo method, Signature signature) {
        return new RecordedListener(
            method.declaringClass().name().toString(),
            method.name(),
            signature.payloadClassName(),
            signature.kind(),
            stringValue(a, "queue", ""),
            stringValue(a, "label", ""),
            stringValue(a, "client", ""),
            stringValue(a, "concurrency", "1"),
            stringValue(a, "batchSize", "1"),
            stringValue(a, "pollInterval", "5s"),
            stringValue(a, "messageInterval", "0s"),
            stringValue(a, "visibilityTimeout", ""),
            stringValue(a, "vtRefresh", "false"),
            enumValue(a, "ackMode", "DELETE"),
            stringValue(a, "maxRetries", "5"),
            stringValue(a, "deadLetterQueue", ""),
            stringValue(a, "autoCreate", "true"),
            stringValue(a, "autoStart", "true"),
            stringValue(a, "fifo", "false"),
            stringValue(a, "schemaVersions", ""),
            enumValue(a, "envelopeValidation", "STRICT"),
            enumValue(a, "unroutable", "DLQ"),
            stringValue(a, "batch", "false"),
            // Not a topic listener: an empty topic keeps the queue attribute authoritative.
            "", "", "SHARED",
            stringValue(a, "raw", "false")
        );
    }
}

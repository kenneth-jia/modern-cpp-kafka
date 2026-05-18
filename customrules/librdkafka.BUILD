load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "rdkafka",

    srcs = glob(
        ["src/**/*.c"],
        exclude = [
            "src/rdkafka_sasl_win32.c",
            "src/rdkafka_sasl_oauthbearer.c",
            "src/rdkafka_sasl_oauthbearer_oidc.c",
            "src/rdkafka_ssl.c",
            "src/rdkafka_sasl_cyrus.c",
            "src/rdkafka_sasl_scram.c",
            "src/rdkafka_zstd.c",
            "src/rdhttp.c",
        ],
    ),

    hdrs = glob([
        "src/**/*.h",
        "src/**/*.c",
        "config.h",
        "librdkafka/rdkafka.h",
    ]),

    includes = ["src", "src/opentelemetry", "."],

    linkopts = ["-lpthread", "-lm", "-ldl", "-lz"],

    visibility = ["//visibility:public"],
)

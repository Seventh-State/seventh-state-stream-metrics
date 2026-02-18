PROJECT             = seventh_state_hello_plugin
PROJECT_DESCRIPTION = Seventh State Hello Plugin
PROJECT_MOD         = seven_hello_plugin_app
PROJECT_VERSION     = 1.0.0

current_rmq_ref     ?= v4.2.x

define PROJECT_APP_EXTRA_KEYS
    {broker_version_requirements, []}
endef


DEPS      = rabbit rabbitmq_management rabbitmq_prometheus
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers rabbitmq_stream_common

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS       = rabbit_common/mk/rabbitmq-plugin.mk

include rabbitmq-components.mk
include erlang.mk

# Set log directory to be under the current project root directory
# Otherwise, erlang.mk will set it to a weird path
CT_LOGS_DIR = $(CURDIR)/logs

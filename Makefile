
LIBIBVERBS_DIR := $(abspath $(CURDIR)/lib/libibverbs)
LIBMLX5_DIR := $(abspath $(CURDIR)/lib/libmlx5)

########
#  List of all modules' directories
########
MODULES   := rdma

########
#  List of all modules' directories
########

########
#  All modules' directories in src and build
########
SRC_DIR   := $(addprefix src/,$(MODULES))
BUILD_DIR := $(addprefix build/,$(MODULES))

########
#  Source and Object files in their  module directories
########
SRC       := $(foreach sdir,$(SRC_DIR),$(wildcard $(sdir)/*.c))
SRCCPP    := $(foreach sdir,$(SRC_DIR),$(wildcard $(sdir)/*.cpp))
OBJ       := $(patsubst src/%.c,build/%.o,$(SRC))
OBJCPP    := $(patsubst src/%.cpp,build/%.o,$(SRCCPP))

CC = gcc -g #-D_GNU_SOURCE # C compiler
CFLAGS = -fPIC -O2 -g -oO
CFLAGS += -I$(LIBMLX5_DIR)/src/
LDFLAGS = -shared  # linking flags

RM = rm -f  # rm command
TARGET_LIB = librdma.so # target lib

INCLUDES  := $(addprefix -I,src/ $(LIBMLX5_DIR)/src)

#RDMA_FLAGS += -DSANITY_CHECK
RDMA_FLAGS += -DDEBUG
#RDMA_FLAGS += -DIBV_RATE_LIMITER
#RDMA_FLAGS += -DMODDED_DRIVER
#RDMA_FLAGS += -DREGISTER_WQ
RDMA_FLAGS += -DEXP_VERBS
#RDMA_FLAGS += -DATOMIC_BE_REPLY
#CPPFLAGS += -DIBV_WRAPPER_INLINE
RDMA_CFLAGS += -D_GNU_SOURCE
RDMA_FLAGS += -fvisibility=default

LDFLAGS += -Wl,-rpath=/usr/local/lib -L/usr/local/lib -Wl,-rpath=/usr/lib -L/usr/lib
LDLIBS = -lrdmacm -libverbs -lpthread -lmlx5

#.PHONY: all
#	all: ${TARGET_LIB}

#$(TARGET_LIB): $(OBJ)
#		$(CC) -g -o $@ $^ ${LDFLAGS} ${LDLIBS}

#$(SRCS:.c=.d):%.d:%.c
#		$(CC) $(CFLAGS) -MM $< >$@

#include $(SRCS:.c=.d)

#.PHONY: clean
#clean:
#	${RM} ${TARGET_LIB} ${OBJS}


########
#  vpath and compile function for each file
########
vpath %.c $(SRC_DIR)
vpath %.cpp $(SRC_DIR)

define make-goal
$1/%.o: %.c
	$(CC) $(INCLUDES) $(RDMA_FLAGS) -fPIC -c $$< -o $$@
$1/%.o: %.cpp
	$(CXX) $(INCLUDES) $(RDMA_FLAGS) -fPIC -c $$< -o $$@
endef

########
#  Phony targets
########
.PHONY: all checkdirs clean

all: checkdirs librdma
checkdirs: $(BUILD_DIR)
	@mkdir -p bin

clean:
	@rm -rf $(BUILD_DIR)

########
#  Create dirs recursively
########
$(BUILD_DIR):
	@mkdir -p $@

########
#  Targets
########
#build/libmlfs.a: $(OBJ)
librdma: $(OBJ) $(OBJCPP)
	ar cr build/librdma.a $(OBJ) $(OBJCPP)
	$(CC) -shared $(DEBUG) -o build/librdma.so $(OBJ) $(OBJCPP) $(LD_FLAGS) $(LDLIBS)


########
#  Compile each source into an object
########
$(foreach bdir,$(BUILD_DIR),$(eval $(call make-goal,$(bdir))))

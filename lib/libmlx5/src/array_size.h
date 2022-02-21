#ifndef ARRAY_SIZE_H
#define ARRAY_SIZE_H

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))
#endif

#endif

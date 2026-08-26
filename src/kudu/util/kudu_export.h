
#ifndef KUDU_EXPORT_H
#define KUDU_EXPORT_H

#include <sstream>

#ifdef KUDU_STATIC_DEFINE
#define KUDU_EXPORT
#else
#ifndef KUDU_EXPORT
#ifdef kudu_client_exported_EXPORTS
/* We are building this library */
#define KUDU_EXPORT __attribute__((visibility("default")))
#else
/* We are using this library */
#define KUDU_EXPORT __attribute__((visibility("default")))
#endif
#endif
#endif

#endif

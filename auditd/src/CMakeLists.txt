add_library(auditd STATIC
            audit.cc audit.h
            auditconfig.cc auditconfig.h
            audit_interface.cc
            auditfile.cc auditfile.h
            configureevent.cc configureevent.h
            event.cc event.h
            eventdescriptor.cc
            eventdescriptor.h)
target_link_libraries(auditd
                      memcached_logger
                      mcd_time
                      mcd_util
                      platform
                      dirutils
                      statistics)
add_dependencies(auditd generate_audit_descriptors)

# auditd is the first target listed in kv_engine which is worth using
# precompiled headers for. All other targers will reuse the already
# compiled headers from auditd (via target_precompile_headers
# REUSE_FROM).
target_precompile_headers(auditd
    PRIVATE ${Memcached_SOURCE_DIR}/precompiled_headers.h)




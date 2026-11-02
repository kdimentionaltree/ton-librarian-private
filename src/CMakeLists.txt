# ton-librarian sources

set(LIBRARIAN_COMMON
    BlockParser.cpp
    Config.cpp
    Storage.cpp
)

# debug info for local sources only (libraries stay as built)
add_compile_options(-g -fno-omit-frame-pointer)

add_executable(ton-librarian
    main.cpp
    LibrarianNode.cpp
    ParseWorker.cpp
    LiteserverSource.cpp
    OverlaySource.cpp
    ${LIBRARIAN_COMMON}
)
target_include_directories(ton-librarian PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(ton-librarian PRIVATE
    tl_api
    tl_lite_api
    lite-client-common
    adnl
    adnllite
    dht
    keyring
    keys
    rldp2
    quic
    overlay
    ton_crypto
    ton_block
    tdutils
    tdactor
    tddb
    rocksdb
)

add_executable(ton-librarian-query
    QueryTool.cpp
    ${LIBRARIAN_COMMON}
)
target_include_directories(ton-librarian-query PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(ton-librarian-query PRIVATE
    tl_api
    ton_crypto
    ton_block
    tdutils
    tdactor
    rocksdb
)

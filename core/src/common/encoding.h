#pragma once

#include <cstddef>
#include <span>
#include <string>

namespace engine {

// Little-endian integer serialization helpers used by the WAL layer.

inline void EncodeUint32(std::span<std::byte, 4> buf, unsigned int v) {
    buf[0] = static_cast<std::byte>(v);
    buf[1] = static_cast<std::byte>(v >> 8);
    buf[2] = static_cast<std::byte>(v >> 16);
    buf[3] = static_cast<std::byte>(v >> 24);
}

inline unsigned int DecodeUint32(std::span<const std::byte, 4> buf) {
    return static_cast<unsigned int>(std::to_integer<unsigned char>(buf[0]))
         | (static_cast<unsigned int>(std::to_integer<unsigned char>(buf[1])) << 8)
         | (static_cast<unsigned int>(std::to_integer<unsigned char>(buf[2])) << 16)
         | (static_cast<unsigned int>(std::to_integer<unsigned char>(buf[3])) << 24);
}

inline void EncodeUint64(std::span<std::byte, 8> buf, unsigned long long v) {
    for (int i = 0; i < 8; ++i)
        buf[static_cast<std::size_t>(i)] = static_cast<std::byte>(v >> (i * 8));
}

inline unsigned long long DecodeUint64(std::span<const std::byte, 8> buf) {
    unsigned long long v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<unsigned long long>(std::to_integer<unsigned char>(
                 buf[static_cast<std::size_t>(i)]))
             << (i * 8);
    return v;
}

// Naive CRC-32 (polynomial 0xEDB88320) — good enough for data integrity checks.
inline unsigned int Crc32(std::span<const std::byte> data) {
    unsigned int crc = 0xFFFFFFFFu;
    for (std::byte b : data) {
        crc ^= std::to_integer<unsigned int>(b);
        for (int j = 0; j < 8; ++j)
            crc = (crc >> 1) ^ (0xEDB88320u & -(crc & 1u));
    }
    return ~crc;
}

inline unsigned int Crc32(const std::string& s) {
    return Crc32(std::span{
        reinterpret_cast<const std::byte*>(s.data()),
        s.size()});
}

} // namespace engine

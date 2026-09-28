---
orphan: true
---

# Jetty ACK Timeout 重建方案（已废弃）

**状态：已废弃。请勿按本文实现或评审。**

权威方案见：

[jetty-single-rebuild-plan.md](./jetty-single-rebuild-plan.md)

废弃原因：本文把「对端 import/bind 同步」写成阻塞前置条件；经对照 kunpeng
`UrmaEndpoint` 数据面（单边 READ/WRITE 打到 remote segment，不依赖对端 jetty
收发语义）后，采用**本端单 Jetty 排空重建、不需要对端协议同步**的方案。

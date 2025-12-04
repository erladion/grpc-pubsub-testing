#ifndef ASYNC_STRUCTS_H
#define ASYNC_STRUCTS_H

class CallData;

enum OpType { CONNECT, READ, WRITE };

struct Tag {
  CallData* connection;
  OpType type;
};

#endif  // ASYNC_STRUCTS_H

#ifndef ASYNC_STRUCTS_H
#define ASYNC_STRUCTS_H

class CallData;  // Forward declaration

enum OpType { CONNECT, READ, WRITE };

// The Tag wraps the pointer and the operation type.
struct Tag {
  CallData* connection;  // Which client is this?
  OpType type;           // What just finished?
};

#endif  // ASYNC_STRUCTS_H

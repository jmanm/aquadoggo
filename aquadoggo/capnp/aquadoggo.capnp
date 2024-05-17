@0xbc5a77c4ac2d28ce;

struct DocumentRequest {
  documentId @0 :Text;
  documentViewId @1 :Text;
}

struct CollectionRequest {
  schemaId @0 :Text;
  filter @1 :List(Field);
  # meta @2
  # orderBy @3
  # orderDirection @4
  # first @5 :UInt32;
  # after @6 :Cursor;
}

struct Field {
  name @0 :Text;
  value :union {
    stringVal @1 :Text;
    intVal @2 :Int32;
    floatVal @3 :Float64;
    boolVal @4 :Bool;
    dataVal @5 :Data;
    relVal @6 :Document;
  }
}

struct Document {
  meta @0 :DocumentMeta;
  fields @1 :List(Field);
}

struct DocumentMeta {
  documentId @0 :Text;
  viewId @1 :Text;
  owner @2 :Text;
}

struct NextArgs {
  logId @0 :UInt64;
  seqNum @1 :UInt64;
  backlink @2 :Text;
  skiplink @3 :Text;
}

interface DocumentRepository {
  publish @0 (entry :Text, operation :Text) -> (nextArgs: NextArgs);
  nextArgs @1 () -> (nextArgs: NextArgs);
}
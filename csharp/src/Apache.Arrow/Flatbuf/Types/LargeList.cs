// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::FlatBuffers;

/// Same as List, but with 64-bit offsets, allowing to represent
/// extremely large data values.
internal struct LargeList : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static LargeList GetRootAsLargeList(ByteBuffer _bb) { return GetRootAsLargeList(_bb, new LargeList()); }
  public static LargeList GetRootAsLargeList(ByteBuffer _bb, LargeList obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p.bb_pos = _i; __p.bb = _bb; }
  public LargeList __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }


  public static void StartLargeList(FlatBufferBuilder builder) { builder.StartObject(0); }
  public static Offset<LargeList> EndLargeList(FlatBufferBuilder builder) {
    int o = builder.EndObject();
    return new Offset<LargeList>(o);
  }
};


}

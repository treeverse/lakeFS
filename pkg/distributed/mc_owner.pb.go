// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        (unknown)
// source: distributed/mc_owner.proto

package distributed

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// message data model for mostly-correct ownership
type MostlyCorrectOwnership struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// owner is a unique identifier for this particular instantiation.  Different concurrent owners
	// must have different owner strings.  Easiest to set it to something random.
	Owner         string                 `protobuf:"bytes,1,opt,name=owner,proto3" json:"owner,omitempty"`
	Expires       *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=expires,proto3" json:"expires,omitempty"`
	Comment       string                 `protobuf:"bytes,3,opt,name=comment,proto3" json:"comment,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *MostlyCorrectOwnership) Reset() {
	*x = MostlyCorrectOwnership{}
	mi := &file_distributed_mc_owner_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MostlyCorrectOwnership) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MostlyCorrectOwnership) ProtoMessage() {}

func (x *MostlyCorrectOwnership) ProtoReflect() protoreflect.Message {
	mi := &file_distributed_mc_owner_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MostlyCorrectOwnership.ProtoReflect.Descriptor instead.
func (*MostlyCorrectOwnership) Descriptor() ([]byte, []int) {
	return file_distributed_mc_owner_proto_rawDescGZIP(), []int{0}
}

func (x *MostlyCorrectOwnership) GetOwner() string {
	if x != nil {
		return x.Owner
	}
	return ""
}

func (x *MostlyCorrectOwnership) GetExpires() *timestamppb.Timestamp {
	if x != nil {
		return x.Expires
	}
	return nil
}

func (x *MostlyCorrectOwnership) GetComment() string {
	if x != nil {
		return x.Comment
	}
	return ""
}

var File_distributed_mc_owner_proto protoreflect.FileDescriptor

const file_distributed_mc_owner_proto_rawDesc = "" +
	"\n" +
	"\x1adistributed/mc_owner.proto\x12\x16io.treeverse.lakefs.kv\x1a\x1fgoogle/protobuf/timestamp.proto\"~\n" +
	"\x16MostlyCorrectOwnership\x12\x14\n" +
	"\x05owner\x18\x01 \x01(\tR\x05owner\x124\n" +
	"\aexpires\x18\x02 \x01(\v2\x1a.google.protobuf.TimestampR\aexpires\x12\x18\n" +
	"\acomment\x18\x03 \x01(\tR\acommentB)Z'github.com/treeverse/lakefs/distributedb\x06proto3"

var (
	file_distributed_mc_owner_proto_rawDescOnce sync.Once
	file_distributed_mc_owner_proto_rawDescData []byte
)

func file_distributed_mc_owner_proto_rawDescGZIP() []byte {
	file_distributed_mc_owner_proto_rawDescOnce.Do(func() {
		file_distributed_mc_owner_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_distributed_mc_owner_proto_rawDesc), len(file_distributed_mc_owner_proto_rawDesc)))
	})
	return file_distributed_mc_owner_proto_rawDescData
}

var file_distributed_mc_owner_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_distributed_mc_owner_proto_goTypes = []any{
	(*MostlyCorrectOwnership)(nil), // 0: io.treeverse.lakefs.kv.MostlyCorrectOwnership
	(*timestamppb.Timestamp)(nil),  // 1: google.protobuf.Timestamp
}
var file_distributed_mc_owner_proto_depIdxs = []int32{
	1, // 0: io.treeverse.lakefs.kv.MostlyCorrectOwnership.expires:type_name -> google.protobuf.Timestamp
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_distributed_mc_owner_proto_init() }
func file_distributed_mc_owner_proto_init() {
	if File_distributed_mc_owner_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_distributed_mc_owner_proto_rawDesc), len(file_distributed_mc_owner_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_distributed_mc_owner_proto_goTypes,
		DependencyIndexes: file_distributed_mc_owner_proto_depIdxs,
		MessageInfos:      file_distributed_mc_owner_proto_msgTypes,
	}.Build()
	File_distributed_mc_owner_proto = out.File
	file_distributed_mc_owner_proto_goTypes = nil
	file_distributed_mc_owner_proto_depIdxs = nil
}

// automatically generated by the FlatBuffers compiler, do not modify


#ifndef FLATBUFFERS_GENERATED_PROTOCOL_RAWDB_H_
#define FLATBUFFERS_GENERATED_PROTOCOL_RAWDB_H_

#include "flatbuffers/flatbuffers.h"

// Ensure the included flatbuffers.h is the same version as when this file was
// generated, otherwise it may not be compatible.
static_assert(FLATBUFFERS_VERSION_MAJOR == 2 &&
              FLATBUFFERS_VERSION_MINOR == 0 &&
              FLATBUFFERS_VERSION_REVISION == 6,
             "Non-compatible flatbuffers version included");

namespace RawDB {

struct flatObj;
struct flatObjBuilder;

struct flatObj FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef flatObjBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_COMMD = 4,
    VT_DB = 6,
    VT_KEY = 8,
    VT_VALUE = 10
  };
  const flatbuffers::String *commd() const {
    return GetPointer<const flatbuffers::String *>(VT_COMMD);
  }
  const flatbuffers::String *db() const {
    return GetPointer<const flatbuffers::String *>(VT_DB);
  }
  const flatbuffers::String *key() const {
    return GetPointer<const flatbuffers::String *>(VT_KEY);
  }
  const flatbuffers::String *value() const {
    return GetPointer<const flatbuffers::String *>(VT_VALUE);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffset(verifier, VT_COMMD) &&
           verifier.VerifyString(commd()) &&
           VerifyOffset(verifier, VT_DB) &&
           verifier.VerifyString(db()) &&
           VerifyOffset(verifier, VT_KEY) &&
           verifier.VerifyString(key()) &&
           VerifyOffset(verifier, VT_VALUE) &&
           verifier.VerifyString(value()) &&
           verifier.EndTable();
  }
};

struct flatObjBuilder {
  typedef flatObj Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_commd(flatbuffers::Offset<flatbuffers::String> commd) {
    fbb_.AddOffset(flatObj::VT_COMMD, commd);
  }
  void add_db(flatbuffers::Offset<flatbuffers::String> db) {
    fbb_.AddOffset(flatObj::VT_DB, db);
  }
  void add_key(flatbuffers::Offset<flatbuffers::String> key) {
    fbb_.AddOffset(flatObj::VT_KEY, key);
  }
  void add_value(flatbuffers::Offset<flatbuffers::String> value) {
    fbb_.AddOffset(flatObj::VT_VALUE, value);
  }
  explicit flatObjBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  flatbuffers::Offset<flatObj> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<flatObj>(end);
    return o;
  }
};

inline flatbuffers::Offset<flatObj> CreateflatObj(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::String> commd = 0,
    flatbuffers::Offset<flatbuffers::String> db = 0,
    flatbuffers::Offset<flatbuffers::String> key = 0,
    flatbuffers::Offset<flatbuffers::String> value = 0) {
  flatObjBuilder builder_(_fbb);
  builder_.add_value(value);
  builder_.add_key(key);
  builder_.add_db(db);
  builder_.add_commd(commd);
  return builder_.Finish();
}

inline flatbuffers::Offset<flatObj> CreateflatObjDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const char *commd = nullptr,
    const char *db = nullptr,
    const char *key = nullptr,
    const char *value = nullptr) {
  auto commd__ = commd ? _fbb.CreateString(commd) : 0;
  auto db__ = db ? _fbb.CreateString(db) : 0;
  auto key__ = key ? _fbb.CreateString(key) : 0;
  auto value__ = value ? _fbb.CreateString(value) : 0;
  return RawDB::CreateflatObj(
      _fbb,
      commd__,
      db__,
      key__,
      value__);
}

inline const RawDB::flatObj *GetflatObj(const void *buf) {
  return flatbuffers::GetRoot<RawDB::flatObj>(buf);
}

inline const RawDB::flatObj *GetSizePrefixedflatObj(const void *buf) {
  return flatbuffers::GetSizePrefixedRoot<RawDB::flatObj>(buf);
}

inline bool VerifyflatObjBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifyBuffer<RawDB::flatObj>(nullptr);
}

inline bool VerifySizePrefixedflatObjBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifySizePrefixedBuffer<RawDB::flatObj>(nullptr);
}

inline void FinishflatObjBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<RawDB::flatObj> root) {
  fbb.Finish(root);
}

inline void FinishSizePrefixedflatObjBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<RawDB::flatObj> root) {
  fbb.FinishSizePrefixed(root);
}

}  // namespace RawDB

#endif  // FLATBUFFERS_GENERATED_PROTOCOL_RAWDB_H_

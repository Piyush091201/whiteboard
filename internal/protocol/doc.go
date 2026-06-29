// Package protocol defines the wire format exchanged between clients and the
// server: a tagged JSON envelope plus the per-type payload structs.
//
// Empty at the scaffold stage. Phase 2 lands the Envelope type and the
// two-stage decode (unmarshal the envelope, switch on Type, then unmarshal the
// json.RawMessage payload into the concrete struct) along with the shape
// operation types used by last-write-wins conflict resolution
// (see docs/adr/0003-conflict-resolution-lww.md).
package protocol

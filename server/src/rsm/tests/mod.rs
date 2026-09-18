//! The tests of `rsm/`. WP-1.1 brings the codec ones; later WPs add their own
//! files here and one line below.
//!
//! - [`samples`] the canonical value of every effect kind and one whole entry.
//!   Everything else is measured against these, so there is one place to look
//!   when a shape changes.
//! - [`roundtrip`] encode → decode → equal, and the layout rules of §5.1.
//! - [`golden`] the fixture bytes under `golden/`: a test that fails on any
//!   byte change (a change there is a FORMAT change and needs a kind version
//!   bump, §5.3).
//! - [`fuzz`] random and mutated bytes into the decoders: they must error,
//!   never panic and never allocate on a length prefix they have not checked.
//! - [`gates`] what the bytes MEAN: the catalogue-version gate over effects
//!   and outcomes alike (I16, D20), the checksum boundary that decides whether
//!   a failure is a torn tail or a committed entry this node cannot apply
//!   (I11), the counters of I18, the distinct request ids of §5.4, and the
//!   values the encoder refuses to write.
//! - [`columns`] the catalogue against the SQL that specifies it: every column
//!   of the one table an effect carries whole has a slot (§8, G-1).

mod columns;
mod fuzz;
mod gates;
mod golden;
mod roundtrip;
mod samples;

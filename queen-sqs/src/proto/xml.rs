//! A one-way XML writer, hand-rolled.
//!
//! CONTRACT. This module RENDERS and never parses. That asymmetry is the whole
//! reason it is a page of code instead of a dependency: the Query protocol takes a
//! FORM body in ([`super::query`]) and answers XML out, so nothing in this
//! facade ever reads an XML document, and a parser's attack surface — entity
//! expansion, external entities, namespace handling — is surface this binary
//! does not need to have.
//!
//! The shape every answer takes:
//!
//! ```xml
//! <SendMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
//!   <SendMessageResult>…</SendMessageResult>
//!   <ResponseMetadata><RequestId>…</RequestId></ResponseMetadata>
//! </SendMessageResponse>
//! ```
//!
//! and every error:
//!
//! ```xml
//! <ErrorResponse xmlns="…">
//!   <Error><Type>Sender</Type><Code>…</Code><Message>…</Message><Detail/></Error>
//!   <RequestId>…</RequestId>
//! </ErrorResponse>
//! ```
//!
//! ESCAPING IS NOT OPTIONAL and it is the one place this file can be wrong in a
//! way that matters: a message body is arbitrary client text on its way back out
//! through `ReceiveMessage`, so `&`, `<`, `>` and both quote forms are escaped on
//! every value — attributes included — and the SQS charset rule (the XML 1.0
//! legal character set) is enforced at SEND, where a body carrying a character
//! XML cannot represent is refused with `InvalidMessageContents` rather than
//! silently mangled on the way back.
//!
//! WHAT THE SEND-SIDE CHECK CANNOT COVER, and what [`escape`] therefore does: a
//! payload written by a NATIVE Queen producer never passed through
//! `SendMessage`, so it can carry `\u{0}`, which no XML 1.0 document may
//! contain. Such a character is replaced with U+FFFD rather than written
//! through. One character is lost either way; the difference is that an invalid
//! document loses the whole answer, and every message in it, at the client's
//! parser.

/// The namespace of the SQS Query API.
pub const NS_SQS: &str = "http://queue.amazonaws.com/doc/2012-11-05/";
/// SNS's, which every SNS answer must carry instead.
pub const NS_SNS: &str = "http://sns.amazonaws.com/doc/2010-03-31/";

/// What a character XML cannot represent becomes.
pub const REPLACEMENT: char = '\u{FFFD}';

/// The prologue every answer opens with. AWS writes one; a client that pins the
/// encoding rather than sniffing it needs it.
const DECLARATION: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

/// One indent level. Two spaces, and the indentation is between elements only —
/// never inside a leaf, where it would become part of a message body.
const INDENT: &str = "  ";

/// An XML document under construction.
///
/// A writer and not a tree: the whole answer is appended in one pass into one
/// pre-sized `String`, the same discipline the broker's own renderers follow.
pub struct Xml {
    out: String,
    /// The elements still open, innermost last. [`Xml::close`] needs the NAME
    /// of what it is closing, which a depth counter cannot supply.
    stack: Vec<String>,
}

impl Xml {
    /// Open a document at `root`, stamping `xmlns`.
    pub fn document(root: &str, namespace: &str) -> Xml {
        let root = element_name(root);
        let mut out = String::with_capacity(512);
        out.push_str(DECLARATION);
        out.push('\n');
        out.push('<');
        out.push_str(&root);
        out.push_str(" xmlns=\"");
        out.push_str(&escape(namespace));
        out.push_str("\">");
        Xml {
            out,
            stack: vec![root.into_owned()],
        }
    }

    /// Open an element.
    pub fn open(&mut self, name: &str) -> &mut Xml {
        let name = element_name(name);
        self.indent(self.stack.len());
        self.out.push('<');
        self.out.push_str(&name);
        self.out.push('>');
        self.stack.push(name.into_owned());
        self
    }

    /// Close the innermost open element. A close with nothing open is a no-op
    /// rather than a panic: this writer is on the answer path of a live
    /// listener, and an unbalanced call is a defect that must not become a 500
    /// for a request that otherwise succeeded.
    pub fn close(&mut self) -> &mut Xml {
        if let Some(name) = self.stack.pop() {
            self.indent(self.stack.len());
            self.out.push_str("</");
            self.out.push_str(&name);
            self.out.push('>');
        }
        self
    }

    /// A leaf with escaped text: `<Name>text</Name>`.
    pub fn leaf(&mut self, name: &str, text: &str) -> &mut Xml {
        let name = element_name(name);
        self.indent(self.stack.len());
        self.out.push('<');
        self.out.push_str(&name);
        self.out.push('>');
        self.out.push_str(&escape(text));
        self.out.push_str("</");
        self.out.push_str(&name);
        self.out.push('>');
        self
    }

    /// A leaf written only when the value is `Some` — an absent optional field
    /// is an ABSENT ELEMENT, never an empty one, because some clients read the
    /// empty string as a value.
    pub fn leaf_opt(&mut self, name: &str, text: Option<&str>) -> &mut Xml {
        match text {
            Some(text) => self.leaf(name, text),
            None => self,
        }
    }

    /// An element with no content at all: `<Detail/>`. AWS writes exactly this
    /// in every `<Error>`, and matching it byte for byte is what makes the
    /// error goldens comparable against a real AWS capture.
    pub fn empty(&mut self, name: &str) -> &mut Xml {
        self.indent(self.stack.len());
        self.out.push('<');
        self.out.push_str(&element_name(name));
        self.out.push_str("/>");
        self
    }

    /// Close everything still open and return the document.
    pub fn finish(mut self) -> String {
        while !self.stack.is_empty() {
            self.close();
        }
        self.out.push('\n');
        self.out
    }

    fn indent(&mut self, depth: usize) {
        self.out.push('\n');
        for _ in 0..depth {
            self.out.push_str(INDENT);
        }
    }
}

/// An element name, reduced to what XML permits: ASCII letters, digits, `_`,
/// `-` and `.`, starting with a letter or `_`.
///
/// Names reach this writer from the action and from the result tables and never
/// from a request body, so nothing today can reach it with a name to reduce.
/// It is unconditional anyway, because the property worth having is not "the
/// current call sites are careful" but **this module cannot emit an ill-formed
/// document** — the one claim that lets the rest of the facade stop thinking
/// about XML. A borrowed name is the common path and allocates nothing.
fn element_name(name: &str) -> std::borrow::Cow<'_, str> {
    fn legal(c: char) -> bool {
        c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')
    }
    fn legal_first(c: char) -> bool {
        c.is_ascii_alphabetic() || c == '_'
    }
    if name.starts_with(legal_first) && name.chars().all(legal) {
        return std::borrow::Cow::Borrowed(name);
    }
    let reduced: String = name.chars().filter(|c| legal(*c)).collect();
    std::borrow::Cow::Owned(match reduced.starts_with(legal_first) {
        true => reduced,
        false => format!("_{reduced}"),
    })
}

/// Escape text for an element body or an attribute value: `&`, `<`, `>`, `"`,
/// `'`. All five, always — the set is not context-dependent here, because the
/// cost of the extra two is nothing and the cost of picking the wrong set for a
/// context is a broken document. A character XML 1.0 cannot represent at all
/// becomes [`REPLACEMENT`]; see the module header for why that is the lesser
/// loss.
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 8);
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c if is_char_safe(c) => out.push(c),
            _ => out.push(REPLACEMENT),
        }
    }
    out
}

/// Whether every character of `text` is legal in an XML 1.0 document. This is
/// the SQS body charset rule, enforced at SEND: `#x9 | #xA | #xD | [#x20-#xD7FF]
/// | [#xE000-#xFFFD] | [#x10000-#x10FFFF]`.
pub fn is_xml_safe(text: &str) -> bool {
    text.chars().all(is_char_safe)
}

/// `text` with every character XML cannot represent replaced, borrowed when
/// there is nothing to replace — which is every message that came in through
/// `SendMessage`.
///
/// WHY THIS IS NOT [`escape`]'S JOB ALONE. A message body ships beside a DIGEST
/// of itself, and every AWS SDK recomputes that digest and throws when it
/// disagrees (Java v2's `MessageMD5ChecksumInterceptor`, and the .NET client).
/// If the substitution happened only inside the writer, the `<Body>` element and
/// its `MD5OfBody` would describe different bytes and the receive would fail
/// inside the SDK, naming the SDK. So the value is made XML-safe FIRST, in one
/// place, and the digest is taken over what will actually be written.
///
/// It applies to what a NATIVE Queen producer wrote and to nothing else: the
/// SQS send path refuses this character set outright, so any message that
/// reaches here with one to replace came from the mixed-consumption path the
/// envelope exists to support.
pub fn sanitize(text: &str) -> std::borrow::Cow<'_, str> {
    if is_xml_safe(text) {
        return std::borrow::Cow::Borrowed(text);
    }
    std::borrow::Cow::Owned(
        text.chars()
            .map(|c| if is_char_safe(c) { c } else { REPLACEMENT })
            .collect(),
    )
}

/// The surrogate range is not tested because a Rust `char` cannot hold one:
/// `str` is UTF-8, and the ranges below are what remains of the production.
fn is_char_safe(c: char) -> bool {
    matches!(c,
        '\u{9}' | '\u{A}' | '\u{D}'
        | '\u{20}'..='\u{D7FF}'
        | '\u{E000}'..='\u{FFFD}'
        | '\u{10000}'..='\u{10FFFF}')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_document_nests_and_finish_closes_what_is_open() {
        let mut x = Xml::document("SendMessageResponse", NS_SQS);
        x.open("SendMessageResult")
            .leaf("MessageId", "id-1")
            .leaf("MD5OfMessageBody", "abc")
            .close();
        // ...and ResponseMetadata is left OPEN on purpose: `finish` is what the
        // renderers rely on to balance the document.
        x.open("ResponseMetadata").leaf("RequestId", "rid");
        let doc = x.finish();
        assert_eq!(
            doc,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <SendMessageResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">\n\
             \x20\x20<SendMessageResult>\n\
             \x20\x20\x20\x20<MessageId>id-1</MessageId>\n\
             \x20\x20\x20\x20<MD5OfMessageBody>abc</MD5OfMessageBody>\n\
             \x20\x20</SendMessageResult>\n\
             \x20\x20<ResponseMetadata>\n\
             \x20\x20\x20\x20<RequestId>rid</RequestId>\n\
             \x20\x20</ResponseMetadata>\n\
             </SendMessageResponse>\n"
        );
    }

    #[test]
    fn all_five_characters_are_escaped() {
        assert_eq!(
            escape(r#"a & b < c > d " e ' f"#),
            "a &amp; b &lt; c &gt; d &quot; e &apos; f"
        );
        // The one that matters: a body is client text on its way back out.
        let mut x = Xml::document("R", NS_SQS);
        x.leaf("Body", "<script>alert(\"x\")</script>");
        assert!(x
            .finish()
            .contains("&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;"));
    }

    #[test]
    fn unicode_survives_and_only_the_illegal_characters_do_not() {
        assert_eq!(escape("héllo → 🐝 café"), "héllo → 🐝 café");
        assert!(is_xml_safe("héllo → 🐝\ttab\nnewline\r"));
        // A native producer's payload can carry these; SendMessage cannot.
        assert!(!is_xml_safe("nul\u{0}byte"));
        assert!(!is_xml_safe("\u{1}\u{8}\u{B}\u{C}\u{E}\u{1F}"));
        assert!(!is_xml_safe("\u{FFFE}"));
        assert_eq!(escape("nul\u{0}byte"), "nul\u{FFFD}byte");
    }

    /// The substitution has to be available BEFORE the writer, because a body
    /// travels beside a digest of itself and the two must describe the same
    /// bytes. Text with nothing to replace is borrowed and unchanged, which is
    /// every message that came in through `SendMessage`.
    #[test]
    fn sanitizing_is_the_writers_own_substitution_applied_early() {
        assert!(matches!(
            sanitize("héllo → 🐝"),
            std::borrow::Cow::Borrowed("héllo → 🐝")
        ));
        assert_eq!(sanitize("nul\u{0}byte"), "nul\u{FFFD}byte");
        assert_eq!(sanitize("\u{1}\u{FFFE}"), "\u{FFFD}\u{FFFD}");
        // The property the digest depends on: sanitizing is a fixed point of
        // what the writer would have done anyway.
        for text in ["plain", "nul\u{0}byte", "\u{B}\u{C}", "a & b"] {
            assert_eq!(escape(&sanitize(text)), escape(text));
            assert!(is_xml_safe(&sanitize(text)));
        }
    }

    #[test]
    fn an_absent_optional_is_an_absent_element_and_empty_is_self_closing() {
        let mut x = Xml::document("R", NS_SQS);
        x.leaf_opt("Present", Some("v")).leaf_opt("Absent", None);
        x.empty("Detail");
        let doc = x.finish();
        assert!(doc.contains("<Present>v</Present>"));
        assert!(!doc.contains("Absent"));
        assert!(doc.contains("<Detail/>"));
    }

    /// The property the rest of the facade relies on: whatever it is handed,
    /// this writer emits a document that parses. Nothing reaches it with a name
    /// like these today — that is exactly why the check is here and not at the
    /// call sites, where it would have to be remembered.
    #[test]
    fn an_element_name_cannot_break_the_document() {
        let mut x = Xml::document("<script>Response", NS_SQS);
        x.open("a b<c").leaf("2Value", "text").close();
        x.empty("");
        let doc = x.finish();
        assert!(!doc.contains("<script>"), "{doc}");
        assert!(doc.contains("<scriptResponse"), "{doc}");
        assert!(
            doc.contains("</scriptResponse>"),
            "an open name and its close agree"
        );
        assert!(doc.contains("<abc>"), "{doc}");
        assert!(doc.contains("<_2Value>text</_2Value>"), "{doc}");
        assert!(doc.contains("<_/>"), "{doc}");
        // Every legal name is passed through untouched, which is the path that
        // matters for the goldens.
        for name in ["SendMessageResult", "MD5OfMessageBody", "x-amz.Thing_1"] {
            assert_eq!(element_name(name), name);
        }
    }

    #[test]
    fn the_namespace_is_escaped_like_every_other_value() {
        let doc = Xml::document("R", "http://x/?a=1&b=2").finish();
        assert!(doc.contains("xmlns=\"http://x/?a=1&amp;b=2\""));
    }
}

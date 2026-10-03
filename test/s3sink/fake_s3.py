#!/usr/bin/env python3
"""A small S3-compatible server for the S3 sink's end-to-end suite.

Standard library only. Path-style addressing (`/<bucket>/<key>`), any
credentials accepted (SigV4 is NOT verified), and objects stored as plain files
under ROOT/<bucket>/<key>, so a test reads the lake straight off the disk the
way a lake reader reads a bucket: with nothing in front of it.

It implements the verbs the sink's client uses
(connectors/queen-s3/src/s3/client.rs) and answers them the way S3 does:

  PUT    /b/k                         PutObject. Content-MD5 checked when sent
                                      (400 BadDigest), x-amz-content-sha256
                                      checked when it is a hex digest
                                      (400 XAmzContentSHA256Mismatch).
                                      ETag: "<md5 hex>".
  POST   /b/k?uploads                 CreateMultipartUpload -> UploadId.
  PUT    /b/k?partNumber=N&uploadId=U UploadPart -> ETag "<md5 hex>".
  POST   /b/k?uploadId=U              CompleteMultipartUpload: parts checked
                                      (InvalidPart, InvalidPartOrder,
                                      EntityTooSmall below 5 MiB except the
                                      last), assembled; ETag = md5 of the
                                      concatenated binary part MD5s + "-N".
  DELETE /b/k?uploadId=U              AbortMultipartUpload (204).
  HEAD   /b/k                         200 + Content-Length/ETag, or 404.
  GET    /b/k                         the body, or 404 NoSuchKey.
  GET    /b?list-type=2               ListObjectsV2: prefix, start-after,
                                      continuation-token, max-keys, delimiter.
  DELETE /b/k                         DeleteObject (204, also when absent).

Every write is atomic: the bytes go to a temporary file under ROOT/.fakes3/tmp
(same filesystem) and are renamed into place, so a reader of ROOT/<bucket>
never sees half an object. Object metadata (ETag, content type) and multipart
uploads in progress live under ROOT/.fakes3/, never inside a bucket directory.

Admin routes, outside the S3 namespace (a bucket name cannot start with an
underscore), all GET:

  /_fake/stats      request counts per operation and status, as JSON
  /_fake/uploads    the multipart uploads in progress (stranded ones too)
  /_fake/delay?ms=2000&count=1&match=<key substring>
                    hold the next matching PUT(s) that long after reading the
                    body and before storing it (a slow gateway); ?clear=1
  /_fake/fail?op=put&status=503&code=SlowDown&count=3&match=..&retryAfter=1
                    answer the next matching request(s) with that error;
                    op is put, get, head, list, delete, multipart_* or *;
                    ?clear=1
  /_fake/inflight   {"put": PUTs in progress, "held": of which delayed}

Every request is logged to stderr as one JSON line (time, method, op, bucket,
decoded key, query, status, and the ETag/bytes of a write), so a test can
count, for instance, how many times one key was PUT.

Usage:
  fake_s3.py --root DIR --port 9000 [--host 127.0.0.1] [--bucket NAME ...]
"""

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import tempfile
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, unquote, urlsplit

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"
MIN_PART_BYTES = 5 * 1024 * 1024
MAX_KEYS = 1000

ROOT = None  # set in main()
META = None
TMP = None
UPLOADS = None

# One lock for the pair (object file, metadata file): a write renames both
# under it and a read takes both under it, so a HEAD never pairs a new body
# with an old ETag.
LOCK = threading.Lock()
STATS_LOCK = threading.Lock()
STATS = {}

# Fault injection (admin route /_fake/delay): hold the next `count` PUTs whose
# key contains `match` for `ms` milliseconds AFTER their body is read and
# BEFORE they are stored — a slow gateway. A held PUT still lands when its
# delay is over, even if the client is gone by then, as a real one would.
DELAY_LOCK = threading.Lock()
DELAYS = []  # [{"ms", "count", "match"}]
INFLIGHT = {"put": 0, "held": 0}


def take_delay(key):
    with DELAY_LOCK:
        for d in DELAYS:
            if d["count"] != 0 and d["match"] in key:
                if d["count"] > 0:
                    d["count"] -= 1
                return d["ms"]
    return 0


class S3Error(Exception):
    def __init__(self, status, code, message, headers=None, **extra):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.headers = headers or {}
        self.extra = extra


# Fault injection (admin route /_fake/fail): answer the next `count` requests
# of operation `op` whose key contains `match` with `status`/`code` (and a
# Retry-After when given), as S3 does when it throttles or has a bad minute.
FAILS = []  # [{"op", "status", "code", "count", "match", "retryAfter"}]


def take_fail(op, key):
    with DELAY_LOCK:
        for f in FAILS:
            if f["count"] != 0 and f["op"] in (op, "*") and f["match"] in (key or ""):
                if f["count"] > 0:
                    f["count"] -= 1
                return dict(f)
    return None


def xml_escape(s):
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def iso8601(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(ts))


def note(op, status):
    with STATS_LOCK:
        key = f"{op} {status}"
        STATS[key] = STATS.get(key, 0) + 1


def bucket_dir(bucket):
    return os.path.join(ROOT, bucket)


def storable(key):
    """Whether a key maps onto a file path. S3 allows `..`, empty segments and
    a trailing slash in a key (`queen/` is the sink's own bucket probe); a
    directory tree cannot hold them as objects. Such a key can never have been
    written here, so a read of it is a plain 404 — what S3 answers for a key
    nobody wrote — and only a WRITE of one is refused."""
    if not key or len(key.encode()) > 1024:
        return False
    return all(
        seg not in ("", ".", "..") and not seg.startswith(".fakes3")
        for seg in key.split("/")
    )


def check_key(key):
    """For writes: refuse what `storable` cannot hold, rather than mangle it."""
    if len(key.encode()) > 1024:
        raise S3Error(400, "KeyTooLongError", "Your key is too long.")
    if not storable(key):
        raise S3Error(
            400,
            "InvalidArgument",
            f"this fake stores keys as files and cannot hold {key!r}",
        )


def object_path(bucket, key):
    return os.path.join(ROOT, bucket, *key.split("/"))


def meta_path(bucket, key):
    return os.path.join(META, bucket, *key.split("/")) + ".meta.json"


def atomic_write(path, chunks):
    """Write `chunks` (an iterable of bytes) to `path` atomically."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=TMP, prefix="w-")
    try:
        with os.fdopen(fd, "wb") as f:
            for c in chunks:
                f.write(c)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def store_object(bucket, key, chunks, etag, content_type):
    path = object_path(bucket, key)
    mpath = meta_path(bucket, key)
    if os.path.isdir(path):
        raise S3Error(
            409, "InvalidArgument", f"{key!r} is a prefix of other keys in this fake"
        )
    # The body goes to a temp file OUTSIDE the lock (it can be large), the two
    # renames happen under it.
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=TMP, prefix="o-")
    size = 0
    try:
        with os.fdopen(fd, "wb") as f:
            for c in chunks:
                size += len(c)
                f.write(c)
            f.flush()
            os.fsync(f.fileno())
        meta = json.dumps(
            {"etag": etag, "contentType": content_type, "size": size}
        ).encode()
        with LOCK:
            atomic_write(mpath, [meta])
            os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return size


def read_meta(bucket, key):
    """(size, etag, content_type, mtime) of an object, or None. Caller holds
    LOCK."""
    if not storable(key):
        return None
    path = object_path(bucket, key)
    if not os.path.isfile(path):
        return None
    st = os.stat(path)
    etag = None
    ctype = "application/octet-stream"
    try:
        with open(meta_path(bucket, key), "rb") as f:
            m = json.loads(f.read())
        etag = m.get("etag")
        ctype = m.get("contentType") or ctype
    except (OSError, ValueError):
        pass
    if etag is None:
        # An object put there by hand: its ETag is its MD5, like S3's.
        with open(path, "rb") as f:
            etag = hashlib.md5(f.read()).hexdigest()
    return st.st_size, etag, ctype, st.st_mtime


def list_keys(bucket):
    base = bucket_dir(bucket)
    out = []
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if not d.startswith(".fakes3")]
        for name in filenames:
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, base)
            out.append(rel.replace(os.sep, "/"))
    out.sort()
    return out


# --- multipart ---------------------------------------------------------------


def upload_dir(upload_id):
    if not re.fullmatch(r"[0-9a-f]{32}", upload_id or ""):
        raise S3Error(404, "NoSuchUpload", "The specified upload does not exist.")
    return os.path.join(UPLOADS, upload_id)


def upload_info(upload_id, bucket, key):
    d = upload_dir(upload_id)
    try:
        with open(os.path.join(d, "info.json"), "rb") as f:
            info = json.loads(f.read())
    except (OSError, ValueError):
        raise S3Error(
            404,
            "NoSuchUpload",
            "The specified upload does not exist. The upload ID may be invalid, or "
            "the upload may have been aborted or completed.",
            UploadId=upload_id,
        ) from None
    if info["bucket"] != bucket or info["key"] != key:
        raise S3Error(404, "NoSuchUpload", "The upload belongs to another key.")
    return d, info


def multipart_etag(part_md5s):
    h = hashlib.md5()
    for m in part_md5s:
        h.update(bytes.fromhex(m))
    return f"{h.hexdigest()}-{len(part_md5s)}"


def parse_complete(body):
    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        raise S3Error(
            400,
            "MalformedXML",
            f"The XML you provided was not well-formed or did not validate: {e}",
        ) from None
    parts = []
    for el in root.iter():
        if not el.tag.endswith("Part"):
            continue
        n = etag = None
        for child in el:
            if child.tag.endswith("PartNumber"):
                n = child.text
            elif child.tag.endswith("ETag"):
                etag = child.text
        if n is None or etag is None:
            raise S3Error(400, "MalformedXML", "a Part without PartNumber or ETag")
        try:
            n = int(n)
        except ValueError:
            raise S3Error(400, "MalformedXML", f"PartNumber {n!r}") from None
        parts.append((n, etag.strip().strip('"')))
    if not parts:
        raise S3Error(400, "MalformedXML", "no Part in CompleteMultipartUpload")
    return parts


# --- the handler -------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "fake-s3/1"

    # quiet the default per-request stderr line; we log our own
    def log_message(self, fmt, *args):
        pass

    def _log(self, op, status, extra=""):
        """One JSON line per request on stderr: what a test reads back to
        count, for instance, how many times one key was PUT."""
        note(op, status)
        bucket, key, query = getattr(self, "_req", (None, None, {}))
        line = {
            "t": round(time.time(), 6),
            "method": self.command,
            "op": op,
            "bucket": bucket,
            "key": key,
            "query": query,
            "status": status,
            "info": extra,
        }
        sys.stderr.write(json.dumps(line, ensure_ascii=False) + "\n")
        sys.stderr.flush()

    # -- plumbing --

    def _read_body(self):
        if "chunked" in (self.headers.get("Transfer-Encoding") or "").lower():
            out = bytearray()
            while True:
                line = self.rfile.readline()
                size = int(line.split(b";")[0].strip(), 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    break
                out += self.rfile.read(size)
                self.rfile.readline()
            return bytes(out)
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def _send(self, status, body=b"", headers=None, content_type="application/xml"):
        self.send_response(status)
        self.send_header("x-amz-request-id", uuid.uuid4().hex[:16].upper())
        self.send_header("Date", formatdate(usegmt=True))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        if status != 204:
            if body or self.command != "HEAD":
                self.send_header("Content-Type", content_type)
            if self.command != "HEAD" or "Content-Length" not in (headers or {}):
                self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _error(self, err, bucket=None, key=None):
        if self.command == "HEAD":
            # HEAD answers carry no body: the status is the whole answer.
            self._send(err.status, b"", err.headers or None)
            return
        resource = "/" + "/".join(p for p in (bucket, key) if p)
        fields = [
            f"<Code>{xml_escape(err.code)}</Code>",
            f"<Message>{xml_escape(err.message)}</Message>",
        ]
        if key:
            fields.append(f"<Key>{xml_escape(key)}</Key>")
        if bucket:
            fields.append(f"<BucketName>{xml_escape(bucket)}</BucketName>")
        for k, v in err.extra.items():
            fields.append(f"<{k}>{xml_escape(str(v))}</{k}>")
        fields.append(f"<Resource>{xml_escape(resource)}</Resource>")
        fields.append(f"<RequestId>{uuid.uuid4().hex[:16].upper()}</RequestId>")
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n<Error>' + "".join(fields) + "</Error>"
        ).encode()
        self._send(err.status, body, err.headers or None)

    def _route(self):
        parts = urlsplit(self.path)
        path = unquote(parts.path)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        segs = path.lstrip("/").split("/", 1)
        bucket = segs[0]
        key = segs[1] if len(segs) > 1 else ""
        return bucket, key, query

    def _operation(self, key, query):
        """The S3 operation a request is, by method, key and query."""
        m = self.command
        if not key:
            return {"GET": "list", "HEAD": "head_bucket"}.get(m)
        if m == "PUT":
            return "multipart_part" if "uploadId" in query else "put"
        if m == "POST":
            if "uploads" in query:
                return "multipart_create"
            if "uploadId" in query:
                return "multipart_complete"
            return None
        if m == "DELETE":
            return "multipart_abort" if "uploadId" in query else "delete"
        return {"HEAD": "head", "GET": "get"}.get(m)

    def _dispatch(self):
        bucket, key, query = self._route()
        self._req = (bucket, key, query)
        op = "?"
        try:
            if bucket == "_fake":
                return self._admin(key)
            if not bucket:
                raise S3Error(400, "InvalidRequest", "this fake serves path-style requests only")
            if not os.path.isdir(bucket_dir(bucket)):
                raise S3Error(404, "NoSuchBucket", "The specified bucket does not exist")
            op = self._operation(key, query)
            if op is None:
                raise S3Error(501, "NotImplemented", f"{self.command} is not implemented here")
            fail = take_fail(op, key)
            if fail is not None:
                if self.command in ("PUT", "POST"):
                    self._read_body()
                headers = {"Retry-After": str(fail["retryAfter"])} if fail.get("retryAfter") else None
                raise S3Error(fail["status"], fail["code"], "injected by /_fake/fail", headers=headers)
            if op == "list":
                return self._list(bucket, query)
            if op == "head_bucket":
                self._send(200)
                return self._log(op, 200)
            if op == "multipart_part":
                return self._upload_part(bucket, key, query)
            if op == "put":
                if self.headers.get("x-amz-copy-source"):
                    raise S3Error(501, "NotImplemented", "CopyObject is not implemented here")
                return self._put(bucket, key)
            if op == "multipart_create":
                return self._create_upload(bucket, key)
            if op == "multipart_complete":
                return self._complete_upload(bucket, key, query)
            if op == "multipart_abort":
                return self._abort_upload(bucket, key, query)
            if op == "delete":
                return self._delete(bucket, key)
            return self._get(bucket, key, head=(op == "head"))
        except S3Error as e:
            # A request body we did not read would desynchronise keep-alive.
            self.close_connection = True
            self._error(e, bucket or None, key or None)
            self._log(op, e.status, e.code)
        except Exception as e:  # noqa: BLE001 — a fake must answer, not die
            self.close_connection = True
            self._error(S3Error(500, "InternalError", f"{type(e).__name__}: {e}"), bucket, key)
            self._log(op, 500, repr(e))

    do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _dispatch

    # -- verbs --

    def _put(self, bucket, key):
        check_key(key)
        body = self._read_body()
        md5 = hashlib.md5(body)
        sent_md5 = self.headers.get("Content-MD5")
        if sent_md5 is not None:
            try:
                want = base64.b64decode(sent_md5, validate=True)
            except ValueError:
                raise S3Error(400, "InvalidDigest", "The Content-MD5 you specified was invalid.") from None
            if len(want) != 16:
                raise S3Error(400, "InvalidDigest", "The Content-MD5 you specified was invalid.")
            if want != md5.digest():
                raise S3Error(
                    400,
                    "BadDigest",
                    "The Content-MD5 you specified did not match what we received.",
                    ExpectedDigest=sent_md5,
                    CalculatedDigest=base64.b64encode(md5.digest()).decode(),
                )
        self._check_sha256(body)
        etag = md5.hexdigest()
        ctype = self.headers.get("Content-Type") or "binary/octet-stream"
        held = take_delay(key)
        with DELAY_LOCK:
            INFLIGHT["put"] += 1
            INFLIGHT["held"] += 1 if held else 0
        try:
            if held:
                time.sleep(held / 1000)
            store_object(bucket, key, [body], etag, ctype)
        finally:
            with DELAY_LOCK:
                INFLIGHT["put"] -= 1
                INFLIGHT["held"] -= 1 if held else 0
        self._send(200, b"", {"ETag": f'"{etag}"'})
        self._log("put", 200, {"bytes": len(body), "etag": etag, "heldMs": held})

    def _check_sha256(self, body):
        claimed = (self.headers.get("x-amz-content-sha256") or "").strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", claimed):
            actual = hashlib.sha256(body).hexdigest()
            if actual != claimed:
                raise S3Error(
                    400,
                    "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.",
                    ClientComputedContentSHA256=claimed,
                    S3ComputedContentSHA256=actual,
                )

    def _get(self, bucket, key, head):
        with LOCK:
            meta = read_meta(bucket, key)
            if meta is None:
                raise S3Error(404, "NoSuchKey", "The specified key does not exist.")
            size, etag, ctype, mtime = meta
            body = b""
            if not head:
                with open(object_path(bucket, key), "rb") as f:
                    body = f.read()
        headers = {
            "ETag": f'"{etag}"',
            "Last-Modified": formatdate(mtime, usegmt=True),
            "Accept-Ranges": "bytes",
        }
        if head:
            headers["Content-Length"] = str(size)
            headers["Content-Type"] = ctype
            self._send(200, b"", headers)
        else:
            self._send(200, body, headers, content_type=ctype)
        self._log("head" if head else "get", 200, {"bytes": size, "etag": etag})

    def _delete(self, bucket, key):
        if storable(key):
            with LOCK:
                for p in (object_path(bucket, key), meta_path(bucket, key)):
                    try:
                        os.unlink(p)
                    except (FileNotFoundError, IsADirectoryError):
                        pass
        self._send(204)
        self._log("delete", 204)

    def _list(self, bucket, query):
        prefix = query.get("prefix", "")
        delimiter = query.get("delimiter", "")
        try:
            max_keys = int(query.get("max-keys", MAX_KEYS))
        except ValueError:
            raise S3Error(400, "InvalidArgument", "max-keys is not a number") from None
        max_keys = max(0, min(max_keys, MAX_KEYS))
        token = query.get("continuation-token")
        start_after = query.get("start-after", "")
        if token is not None:
            try:
                after = base64.urlsafe_b64decode(token.encode()).decode()
            except ValueError:
                raise S3Error(400, "InvalidArgument", "The continuation token provided is incorrect") from None
        else:
            after = start_after
        with LOCK:
            keys = [k for k in list_keys(bucket) if k.startswith(prefix) and k > after]
            entries = []  # (key or common prefix, is_prefix)
            truncated = False
            # The cursor: the last KEY this page covered. A common prefix covers
            # every key under it, so the cursor jumps to the last of them and the
            # next page never repeats the prefix.
            last = None
            i = 0
            while i < len(keys):
                k = keys[i]
                if len(entries) >= max_keys:
                    truncated = True
                    break
                cut = k.find(delimiter, len(prefix)) if delimiter else -1
                if cut >= 0:
                    cp = k[: cut + len(delimiter)]
                    while i < len(keys) and keys[i].startswith(cp):
                        i += 1
                    entries.append((cp, True))
                    last = keys[i - 1]
                    continue
                entries.append((k, False))
                last = k
                i += 1
            contents = []
            for k, is_prefix in entries:
                if is_prefix:
                    continue
                meta = read_meta(bucket, k)
                if meta is None:
                    continue
                size, etag, _, mtime = meta
                contents.append(
                    "<Contents>"
                    f"<Key>{xml_escape(k)}</Key>"
                    f"<LastModified>{iso8601(mtime)}</LastModified>"
                    f"<ETag>&quot;{xml_escape(etag)}&quot;</ETag>"
                    f"<Size>{size}</Size>"
                    "<StorageClass>STANDARD</StorageClass>"
                    "</Contents>"
                )
        prefixes = [
            f"<CommonPrefixes><Prefix>{xml_escape(k)}</Prefix></CommonPrefixes>"
            for k, is_prefix in entries
            if is_prefix
        ]
        out = [
            '<?xml version="1.0" encoding="UTF-8"?>\n',
            f'<ListBucketResult xmlns="{S3_NS}">',
            f"<Name>{xml_escape(bucket)}</Name>",
            f"<Prefix>{xml_escape(prefix)}</Prefix>",
        ]
        if token is not None:
            out.append(f"<ContinuationToken>{xml_escape(token)}</ContinuationToken>")
        if start_after:
            out.append(f"<StartAfter>{xml_escape(start_after)}</StartAfter>")
        out.append(f"<KeyCount>{len(entries)}</KeyCount>")
        out.append(f"<MaxKeys>{max_keys}</MaxKeys>")
        if delimiter:
            out.append(f"<Delimiter>{xml_escape(delimiter)}</Delimiter>")
        out.append(f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>")
        if truncated and last is not None:
            nxt = base64.urlsafe_b64encode(last.encode()).decode()
            out.append(f"<NextContinuationToken>{nxt}</NextContinuationToken>")
        out.extend(contents)
        out.extend(prefixes)
        out.append("</ListBucketResult>")
        self._send(200, "".join(out).encode())
        self._log("list", 200, {"prefix": prefix, "keys": len(entries), "truncated": truncated})

    def _create_upload(self, bucket, key):
        check_key(key)
        self._read_body()
        upload_id = uuid.uuid4().hex
        d = os.path.join(UPLOADS, upload_id)
        os.makedirs(d)
        info = {
            "bucket": bucket,
            "key": key,
            "contentType": self.headers.get("Content-Type") or "binary/octet-stream",
            "initiated": time.time(),
        }
        atomic_write(os.path.join(d, "info.json"), [json.dumps(info).encode()])
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<InitiateMultipartUploadResult xmlns="{S3_NS}">'
            f"<Bucket>{xml_escape(bucket)}</Bucket>"
            f"<Key>{xml_escape(key)}</Key>"
            f"<UploadId>{upload_id}</UploadId>"
            "</InitiateMultipartUploadResult>"
        ).encode()
        self._send(200, body)
        self._log("multipart_create", 200, {"uploadId": upload_id})

    def _upload_part(self, bucket, key, query):
        check_key(key)
        d, _ = upload_info(query.get("uploadId"), bucket, key)
        try:
            n = int(query.get("partNumber", ""))
        except ValueError:
            raise S3Error(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive") from None
        if not 1 <= n <= 10000:
            raise S3Error(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive")
        body = self._read_body()
        sent_md5 = self.headers.get("Content-MD5")
        md5 = hashlib.md5(body)
        if sent_md5 is not None:
            try:
                want = base64.b64decode(sent_md5, validate=True)
            except ValueError:
                raise S3Error(400, "InvalidDigest", "The Content-MD5 you specified was invalid.") from None
            if want != md5.digest():
                raise S3Error(400, "BadDigest", "The Content-MD5 you specified did not match what we received.")
        self._check_sha256(body)
        etag = md5.hexdigest()
        atomic_write(os.path.join(d, f"part-{n:05d}"), [body])
        atomic_write(os.path.join(d, f"part-{n:05d}.etag"), [etag.encode()])
        self._send(200, b"", {"ETag": f'"{etag}"'})
        self._log("multipart_part", 200, {"part": n, "bytes": len(body), "etag": etag, "uploadId": query.get("uploadId")})

    def _complete_upload(self, bucket, key, query):
        check_key(key)
        upload_id = query.get("uploadId")
        d, info = upload_info(upload_id, bucket, key)
        parts = parse_complete(self._read_body())
        prev = 0
        for n, _ in parts:
            if n <= prev:
                raise S3Error(400, "InvalidPartOrder", "The list of parts was not in ascending order. The parts list must be specified in order by part number.")
            prev = n
        files = []
        md5s = []
        for i, (n, etag) in enumerate(parts):
            pf = os.path.join(d, f"part-{n:05d}")
            try:
                with open(pf + ".etag", "rb") as f:
                    have = f.read().decode()
                size = os.path.getsize(pf)
            except OSError:
                raise S3Error(400, "InvalidPart", "One or more of the specified parts could not be found.", UploadId=upload_id, PartNumber=n) from None
            if have != etag:
                raise S3Error(400, "InvalidPart", "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.", UploadId=upload_id, PartNumber=n, ETag=etag)
            if i < len(parts) - 1 and size < MIN_PART_BYTES:
                raise S3Error(400, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed size", ProposedSize=size, MinSizeAllowed=MIN_PART_BYTES, PartNumber=n)
            files.append(pf)
            md5s.append(have)

        def chunks():
            for pf in files:
                with open(pf, "rb") as f:
                    while True:
                        c = f.read(1 << 20)
                        if not c:
                            break
                        yield c

        etag = multipart_etag(md5s)
        size = store_object(bucket, key, chunks(), etag, info["contentType"])
        for name in os.listdir(d):
            os.unlink(os.path.join(d, name))
        os.rmdir(d)
        # S3 escapes the quotes of the ETag inside the XML document.
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<CompleteMultipartUploadResult xmlns="{S3_NS}">'
            f"<Location>http://{xml_escape(self.headers.get('Host') or 'localhost')}/{xml_escape(bucket)}/{xml_escape(key)}</Location>"
            f"<Bucket>{xml_escape(bucket)}</Bucket>"
            f"<Key>{xml_escape(key)}</Key>"
            f"<ETag>&quot;{etag}&quot;</ETag>"
            "</CompleteMultipartUploadResult>"
        ).encode()
        self._send(200, body)
        self._log("multipart_complete", 200, {"parts": len(parts), "bytes": size, "etag": etag, "uploadId": upload_id})

    def _abort_upload(self, bucket, key, query):
        check_key(key)
        d, _ = upload_info(query.get("uploadId"), bucket, key)
        for name in os.listdir(d):
            os.unlink(os.path.join(d, name))
        os.rmdir(d)
        self._send(204)
        self._log("multipart_abort", 204, {"uploadId": query.get("uploadId")})

    # -- admin --

    def _admin(self, what):
        if self.command != "GET":
            raise S3Error(405, "MethodNotAllowed", "admin routes are GET")
        _, _, query = self._req
        if what == "delay":
            # /_fake/delay?ms=2000&count=1&match=/queue=   (count -1: until cleared)
            # /_fake/delay?clear=1
            with DELAY_LOCK:
                if query.get("clear"):
                    DELAYS.clear()
                else:
                    DELAYS.append(
                        {
                            "ms": int(query.get("ms", "1000")),
                            "count": int(query.get("count", "1")),
                            "match": query.get("match", ""),
                        }
                    )
                body = json.dumps({"delays": DELAYS, "inflight": INFLIGHT}).encode()
        elif what == "fail":
            # /_fake/fail?op=put&status=503&code=SlowDown&count=3&match=/queue=&retryAfter=1
            # /_fake/fail?clear=1        (op: put, get, head, list, multipart_*, delete, or *)
            with DELAY_LOCK:
                if query.get("clear"):
                    FAILS.clear()
                else:
                    FAILS.append(
                        {
                            "op": query.get("op", "put"),
                            "status": int(query.get("status", "503")),
                            "code": query.get("code", "SlowDown"),
                            "count": int(query.get("count", "1")),
                            "match": query.get("match", ""),
                            "retryAfter": int(query.get("retryAfter", "0")),
                        }
                    )
                body = json.dumps({"fails": FAILS}).encode()
        elif what == "inflight":
            with DELAY_LOCK:
                body = json.dumps(INFLIGHT).encode()
        elif what == "stats":
            with STATS_LOCK:
                body = json.dumps(STATS, sort_keys=True).encode()
        elif what == "uploads":
            out = []
            for upload_id in sorted(os.listdir(UPLOADS)):
                d = os.path.join(UPLOADS, upload_id)
                try:
                    with open(os.path.join(d, "info.json"), "rb") as f:
                        info = json.loads(f.read())
                except (OSError, ValueError):
                    continue
                parts = sorted(n for n in os.listdir(d) if re.fullmatch(r"part-\d{5}", n))
                info["uploadId"] = upload_id
                info["parts"] = [
                    {"part": int(p[5:]), "bytes": os.path.getsize(os.path.join(d, p))}
                    for p in parts
                ]
                out.append(info)
            body = json.dumps(out).encode()
        else:
            raise S3Error(404, "NoSuchKey", "unknown admin route")
        self._send(200, body, content_type="application/json")
        self._log("admin", 200, what)


def main():
    global ROOT, META, TMP, UPLOADS
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--root", required=True, help="directory holding the buckets")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument(
        "--bucket", action="append", default=[], help="create this bucket (repeatable)"
    )
    args = ap.parse_args()
    ROOT = os.path.abspath(args.root)
    META = os.path.join(ROOT, ".fakes3", "meta")
    TMP = os.path.join(ROOT, ".fakes3", "tmp")
    UPLOADS = os.path.join(ROOT, ".fakes3", "uploads")
    for d in (ROOT, META, TMP, UPLOADS):
        os.makedirs(d, exist_ok=True)
    for b in args.bucket:
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", b):
            ap.error(f"{b!r} is not a valid bucket name")
        os.makedirs(os.path.join(ROOT, b), exist_ok=True)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer.request_queue_size = 256
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"fake-s3 listening on http://{args.host}:{args.port} root={ROOT}", flush=True)
    try:
        srv.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

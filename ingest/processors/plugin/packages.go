// +build linux,go1.16,!go1.17,!386,!arm,!mips,!mipsle,!s390x

package plugin

import (
	tar "archive/tar"
	zip "archive/zip"
	"bufio"
	"bytes"
	bzip2 "compress/bzip2"
	flate "compress/flate"
	gzip "compress/gzip"
	lzw "compress/lzw"
	zlib "compress/zlib"
	heap "container/heap"
	list "container/list"
	ring "container/ring"
	"context"
	"crypto"
	aes "crypto/aes"
	cipher "crypto/cipher"
	des "crypto/des"
	dsa "crypto/dsa"
	ecdsa "crypto/ecdsa"
	elliptic "crypto/elliptic"
	hmac "crypto/hmac"
	md5 "crypto/md5"
	rand "crypto/rand"
	rc4 "crypto/rc4"
	rsa "crypto/rsa"
	sha1 "crypto/sha1"
	sha256 "crypto/sha256"
	sha512 "crypto/sha512"
	subtle "crypto/subtle"
	tls "crypto/tls"
	x509 "crypto/x509"
	pkix "crypto/x509/pkix"
	"encoding"
	ascii85 "encoding/ascii85"
	asn1 "encoding/asn1"
	base32 "encoding/base32"
	base64 "encoding/base64"
	binary "encoding/binary"
	csv "encoding/csv"
	gob "encoding/gob"
	hex "encoding/hex"
	json "encoding/json"
	pem "encoding/pem"
	xml "encoding/xml"
	"errors"
	"expvar"
	"flag"
	"fmt"
	rfc5424 "github.com/crewjam/rfc5424"
	safefile "github.com/dchest/safefile"
	glob "github.com/gobwas/glob"
	flock "github.com/gofrs/flock"
	gopacket "github.com/google/gopacket"
	renameio "github.com/google/renameio"
	uuid "github.com/google/uuid"
	ingest "github.com/gravwell/gravwell/v3/ingest"
	config "github.com/gravwell/gravwell/v3/ingest/config"
	entry "github.com/gravwell/gravwell/v3/ingest/entry"
	ipfix "github.com/gravwell/ipfix"
	filetype "github.com/h2non/filetype"
	ipmigo "github.com/k-sone/ipmigo"
	compress "github.com/klauspost/compress"
	msgraph "github.com/open-networks/go-msgraph"
	xlsx "github.com/tealeg/xlsx"
	ast "go/ast"
	build "go/build"
	constant "go/constant"
	doc "go/doc"
	format "go/format"
	importer "go/importer"
	parser "go/parser"
	printer "go/printer"
	scanner "go/scanner"
	token "go/token"
	types "go/types"
	"hash"
	adler32 "hash/adler32"
	crc32 "hash/crc32"
	crc64 "hash/crc64"
	fnv "hash/fnv"
	maphash "hash/maphash"
	"html"
	template "html/template"
	"image"
	color "image/color"
	palette "image/color/palette"
	draw "image/draw"
	gif "image/gif"
	jpeg "image/jpeg"
	png "image/png"
	suffixarray "index/suffixarray"
	"io"
	fs "io/fs"
	ioutil "io/ioutil"
	"log"
	syslog "log/syslog"
	"math"
	big "math/big"
	bits "math/bits"
	cmplx "math/cmplx"
	rand_ "math/rand"
	"mime"
	multipart "mime/multipart"
	quotedprintable "mime/quotedprintable"
	"net"
	http "net/http"
	cgi "net/http/cgi"
	cookiejar "net/http/cookiejar"
	fcgi "net/http/fcgi"
	httptest "net/http/httptest"
	httptrace "net/http/httptrace"
	httputil "net/http/httputil"
	pprof "net/http/pprof"
	mail "net/mail"
	rpc "net/rpc"
	jsonrpc "net/rpc/jsonrpc"
	smtp "net/smtp"
	textproto "net/textproto"
	url "net/url"
	"os"
	exec "os/exec"
	user "os/user"
	"path"
	filepath "path/filepath"
	"reflect"
	"regexp"
	syntax "regexp/syntax"
	debug "runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	atomic "sync/atomic"
	scanner_ "text/scanner"
	tabwriter "text/tabwriter"
	template_ "text/template"
	parse "text/template/parse"
	"time"
	_ "time/tzdata"
	"unicode"
	utf16 "unicode/utf16"
	utf8 "unicode/utf8"
)

import "github.com/open2b/scriggo/native"

func init() {
	packages = make(native.Packages, 142)
	var decs native.Declarations
	// "archive/tar"
	decs = make(native.Declarations, 29)
	decs["ErrFieldTooLong"] = &tar.ErrFieldTooLong
	decs["ErrHeader"] = &tar.ErrHeader
	decs["ErrWriteAfterClose"] = &tar.ErrWriteAfterClose
	decs["ErrWriteTooLong"] = &tar.ErrWriteTooLong
	decs["FileInfoHeader"] = tar.FileInfoHeader
	decs["Format"] = reflect.TypeOf((*tar.Format)(nil)).Elem()
	decs["FormatGNU"] = tar.FormatGNU
	decs["FormatPAX"] = tar.FormatPAX
	decs["FormatUSTAR"] = tar.FormatUSTAR
	decs["FormatUnknown"] = tar.FormatUnknown
	decs["Header"] = reflect.TypeOf((*tar.Header)(nil)).Elem()
	decs["NewReader"] = tar.NewReader
	decs["NewWriter"] = tar.NewWriter
	decs["Reader"] = reflect.TypeOf((*tar.Reader)(nil)).Elem()
	decs["TypeBlock"] = native.UntypedNumericConst("52")
	decs["TypeChar"] = native.UntypedNumericConst("51")
	decs["TypeCont"] = native.UntypedNumericConst("55")
	decs["TypeDir"] = native.UntypedNumericConst("53")
	decs["TypeFifo"] = native.UntypedNumericConst("54")
	decs["TypeGNULongLink"] = native.UntypedNumericConst("75")
	decs["TypeGNULongName"] = native.UntypedNumericConst("76")
	decs["TypeGNUSparse"] = native.UntypedNumericConst("83")
	decs["TypeLink"] = native.UntypedNumericConst("49")
	decs["TypeReg"] = native.UntypedNumericConst("48")
	decs["TypeRegA"] = native.UntypedNumericConst("0")
	decs["TypeSymlink"] = native.UntypedNumericConst("50")
	decs["TypeXGlobalHeader"] = native.UntypedNumericConst("103")
	decs["TypeXHeader"] = native.UntypedNumericConst("120")
	decs["Writer"] = reflect.TypeOf((*tar.Writer)(nil)).Elem()
	packages["archive/tar"] = native.Package{
		Name:         "tar",
		Declarations: decs,
	}
	// "archive/zip"
	decs = make(native.Declarations, 18)
	decs["Compressor"] = reflect.TypeOf((*zip.Compressor)(nil)).Elem()
	decs["Decompressor"] = reflect.TypeOf((*zip.Decompressor)(nil)).Elem()
	decs["Deflate"] = zip.Deflate
	decs["ErrAlgorithm"] = &zip.ErrAlgorithm
	decs["ErrChecksum"] = &zip.ErrChecksum
	decs["ErrFormat"] = &zip.ErrFormat
	decs["File"] = reflect.TypeOf((*zip.File)(nil)).Elem()
	decs["FileHeader"] = reflect.TypeOf((*zip.FileHeader)(nil)).Elem()
	decs["FileInfoHeader"] = zip.FileInfoHeader
	decs["NewReader"] = zip.NewReader
	decs["NewWriter"] = zip.NewWriter
	decs["OpenReader"] = zip.OpenReader
	decs["ReadCloser"] = reflect.TypeOf((*zip.ReadCloser)(nil)).Elem()
	decs["Reader"] = reflect.TypeOf((*zip.Reader)(nil)).Elem()
	decs["RegisterCompressor"] = zip.RegisterCompressor
	decs["RegisterDecompressor"] = zip.RegisterDecompressor
	decs["Store"] = zip.Store
	decs["Writer"] = reflect.TypeOf((*zip.Writer)(nil)).Elem()
	packages["archive/zip"] = native.Package{
		Name:         "zip",
		Declarations: decs,
	}
	// "bufio"
	decs = make(native.Declarations, 25)
	decs["ErrAdvanceTooFar"] = &bufio.ErrAdvanceTooFar
	decs["ErrBadReadCount"] = &bufio.ErrBadReadCount
	decs["ErrBufferFull"] = &bufio.ErrBufferFull
	decs["ErrFinalToken"] = &bufio.ErrFinalToken
	decs["ErrInvalidUnreadByte"] = &bufio.ErrInvalidUnreadByte
	decs["ErrInvalidUnreadRune"] = &bufio.ErrInvalidUnreadRune
	decs["ErrNegativeAdvance"] = &bufio.ErrNegativeAdvance
	decs["ErrNegativeCount"] = &bufio.ErrNegativeCount
	decs["ErrTooLong"] = &bufio.ErrTooLong
	decs["MaxScanTokenSize"] = native.UntypedNumericConst("65536")
	decs["NewReadWriter"] = bufio.NewReadWriter
	decs["NewReader"] = bufio.NewReader
	decs["NewReaderSize"] = bufio.NewReaderSize
	decs["NewScanner"] = bufio.NewScanner
	decs["NewWriter"] = bufio.NewWriter
	decs["NewWriterSize"] = bufio.NewWriterSize
	decs["ReadWriter"] = reflect.TypeOf((*bufio.ReadWriter)(nil)).Elem()
	decs["Reader"] = reflect.TypeOf((*bufio.Reader)(nil)).Elem()
	decs["ScanBytes"] = bufio.ScanBytes
	decs["ScanLines"] = bufio.ScanLines
	decs["ScanRunes"] = bufio.ScanRunes
	decs["ScanWords"] = bufio.ScanWords
	decs["Scanner"] = reflect.TypeOf((*bufio.Scanner)(nil)).Elem()
	decs["SplitFunc"] = reflect.TypeOf((*bufio.SplitFunc)(nil)).Elem()
	decs["Writer"] = reflect.TypeOf((*bufio.Writer)(nil)).Elem()
	packages["bufio"] = native.Package{
		Name:         "bufio",
		Declarations: decs,
	}
	// "bytes"
	decs = make(native.Declarations, 54)
	decs["Buffer"] = reflect.TypeOf((*bytes.Buffer)(nil)).Elem()
	decs["Compare"] = bytes.Compare
	decs["Contains"] = bytes.Contains
	decs["ContainsAny"] = bytes.ContainsAny
	decs["ContainsRune"] = bytes.ContainsRune
	decs["Count"] = bytes.Count
	decs["Equal"] = bytes.Equal
	decs["EqualFold"] = bytes.EqualFold
	decs["ErrTooLarge"] = &bytes.ErrTooLarge
	decs["Fields"] = bytes.Fields
	decs["FieldsFunc"] = bytes.FieldsFunc
	decs["HasPrefix"] = bytes.HasPrefix
	decs["HasSuffix"] = bytes.HasSuffix
	decs["Index"] = bytes.Index
	decs["IndexAny"] = bytes.IndexAny
	decs["IndexByte"] = bytes.IndexByte
	decs["IndexFunc"] = bytes.IndexFunc
	decs["IndexRune"] = bytes.IndexRune
	decs["Join"] = bytes.Join
	decs["LastIndex"] = bytes.LastIndex
	decs["LastIndexAny"] = bytes.LastIndexAny
	decs["LastIndexByte"] = bytes.LastIndexByte
	decs["LastIndexFunc"] = bytes.LastIndexFunc
	decs["Map"] = bytes.Map
	decs["MinRead"] = native.UntypedNumericConst("512")
	decs["NewBuffer"] = bytes.NewBuffer
	decs["NewBufferString"] = bytes.NewBufferString
	decs["NewReader"] = bytes.NewReader
	decs["Reader"] = reflect.TypeOf((*bytes.Reader)(nil)).Elem()
	decs["Repeat"] = bytes.Repeat
	decs["Replace"] = bytes.Replace
	decs["ReplaceAll"] = bytes.ReplaceAll
	decs["Runes"] = bytes.Runes
	decs["Split"] = bytes.Split
	decs["SplitAfter"] = bytes.SplitAfter
	decs["SplitAfterN"] = bytes.SplitAfterN
	decs["SplitN"] = bytes.SplitN
	decs["Title"] = bytes.Title
	decs["ToLower"] = bytes.ToLower
	decs["ToLowerSpecial"] = bytes.ToLowerSpecial
	decs["ToTitle"] = bytes.ToTitle
	decs["ToTitleSpecial"] = bytes.ToTitleSpecial
	decs["ToUpper"] = bytes.ToUpper
	decs["ToUpperSpecial"] = bytes.ToUpperSpecial
	decs["ToValidUTF8"] = bytes.ToValidUTF8
	decs["Trim"] = bytes.Trim
	decs["TrimFunc"] = bytes.TrimFunc
	decs["TrimLeft"] = bytes.TrimLeft
	decs["TrimLeftFunc"] = bytes.TrimLeftFunc
	decs["TrimPrefix"] = bytes.TrimPrefix
	decs["TrimRight"] = bytes.TrimRight
	decs["TrimRightFunc"] = bytes.TrimRightFunc
	decs["TrimSpace"] = bytes.TrimSpace
	decs["TrimSuffix"] = bytes.TrimSuffix
	packages["bytes"] = native.Package{
		Name:         "bytes",
		Declarations: decs,
	}
	// "compress/bzip2"
	decs = make(native.Declarations, 2)
	decs["NewReader"] = bzip2.NewReader
	decs["StructuralError"] = reflect.TypeOf((*bzip2.StructuralError)(nil)).Elem()
	packages["compress/bzip2"] = native.Package{
		Name:         "bzip2",
		Declarations: decs,
	}
	// "compress/flate"
	decs = make(native.Declarations, 16)
	decs["BestCompression"] = native.UntypedNumericConst("9")
	decs["BestSpeed"] = native.UntypedNumericConst("1")
	decs["CorruptInputError"] = reflect.TypeOf((*flate.CorruptInputError)(nil)).Elem()
	decs["DefaultCompression"] = native.UntypedNumericConst("-1")
	decs["HuffmanOnly"] = native.UntypedNumericConst("-2")
	decs["InternalError"] = reflect.TypeOf((*flate.InternalError)(nil)).Elem()
	decs["NewReader"] = flate.NewReader
	decs["NewReaderDict"] = flate.NewReaderDict
	decs["NewWriter"] = flate.NewWriter
	decs["NewWriterDict"] = flate.NewWriterDict
	decs["NoCompression"] = native.UntypedNumericConst("0")
	decs["ReadError"] = reflect.TypeOf((*flate.ReadError)(nil)).Elem()
	decs["Reader"] = reflect.TypeOf((*flate.Reader)(nil)).Elem()
	decs["Resetter"] = reflect.TypeOf((*flate.Resetter)(nil)).Elem()
	decs["WriteError"] = reflect.TypeOf((*flate.WriteError)(nil)).Elem()
	decs["Writer"] = reflect.TypeOf((*flate.Writer)(nil)).Elem()
	packages["compress/flate"] = native.Package{
		Name:         "flate",
		Declarations: decs,
	}
	// "compress/gzip"
	decs = make(native.Declarations, 13)
	decs["BestCompression"] = native.UntypedNumericConst("9")
	decs["BestSpeed"] = native.UntypedNumericConst("1")
	decs["DefaultCompression"] = native.UntypedNumericConst("-1")
	decs["ErrChecksum"] = &gzip.ErrChecksum
	decs["ErrHeader"] = &gzip.ErrHeader
	decs["Header"] = reflect.TypeOf((*gzip.Header)(nil)).Elem()
	decs["HuffmanOnly"] = native.UntypedNumericConst("-2")
	decs["NewReader"] = gzip.NewReader
	decs["NewWriter"] = gzip.NewWriter
	decs["NewWriterLevel"] = gzip.NewWriterLevel
	decs["NoCompression"] = native.UntypedNumericConst("0")
	decs["Reader"] = reflect.TypeOf((*gzip.Reader)(nil)).Elem()
	decs["Writer"] = reflect.TypeOf((*gzip.Writer)(nil)).Elem()
	packages["compress/gzip"] = native.Package{
		Name:         "gzip",
		Declarations: decs,
	}
	// "compress/lzw"
	decs = make(native.Declarations, 5)
	decs["LSB"] = lzw.LSB
	decs["MSB"] = lzw.MSB
	decs["NewReader"] = lzw.NewReader
	decs["NewWriter"] = lzw.NewWriter
	decs["Order"] = reflect.TypeOf((*lzw.Order)(nil)).Elem()
	packages["compress/lzw"] = native.Package{
		Name:         "lzw",
		Declarations: decs,
	}
	// "compress/zlib"
	decs = make(native.Declarations, 15)
	decs["BestCompression"] = native.UntypedNumericConst("9")
	decs["BestSpeed"] = native.UntypedNumericConst("1")
	decs["DefaultCompression"] = native.UntypedNumericConst("-1")
	decs["ErrChecksum"] = &zlib.ErrChecksum
	decs["ErrDictionary"] = &zlib.ErrDictionary
	decs["ErrHeader"] = &zlib.ErrHeader
	decs["HuffmanOnly"] = native.UntypedNumericConst("-2")
	decs["NewReader"] = zlib.NewReader
	decs["NewReaderDict"] = zlib.NewReaderDict
	decs["NewWriter"] = zlib.NewWriter
	decs["NewWriterLevel"] = zlib.NewWriterLevel
	decs["NewWriterLevelDict"] = zlib.NewWriterLevelDict
	decs["NoCompression"] = native.UntypedNumericConst("0")
	decs["Resetter"] = reflect.TypeOf((*zlib.Resetter)(nil)).Elem()
	decs["Writer"] = reflect.TypeOf((*zlib.Writer)(nil)).Elem()
	packages["compress/zlib"] = native.Package{
		Name:         "zlib",
		Declarations: decs,
	}
	// "container/heap"
	decs = make(native.Declarations, 6)
	decs["Fix"] = heap.Fix
	decs["Init"] = heap.Init
	decs["Interface"] = reflect.TypeOf((*heap.Interface)(nil)).Elem()
	decs["Pop"] = heap.Pop
	decs["Push"] = heap.Push
	decs["Remove"] = heap.Remove
	packages["container/heap"] = native.Package{
		Name:         "heap",
		Declarations: decs,
	}
	// "container/list"
	decs = make(native.Declarations, 3)
	decs["Element"] = reflect.TypeOf((*list.Element)(nil)).Elem()
	decs["List"] = reflect.TypeOf((*list.List)(nil)).Elem()
	decs["New"] = list.New
	packages["container/list"] = native.Package{
		Name:         "list",
		Declarations: decs,
	}
	// "container/ring"
	decs = make(native.Declarations, 2)
	decs["New"] = ring.New
	decs["Ring"] = reflect.TypeOf((*ring.Ring)(nil)).Elem()
	packages["container/ring"] = native.Package{
		Name:         "ring",
		Declarations: decs,
	}
	// "context"
	decs = make(native.Declarations, 10)
	decs["Background"] = context.Background
	decs["CancelFunc"] = reflect.TypeOf((*context.CancelFunc)(nil)).Elem()
	decs["Canceled"] = &context.Canceled
	decs["Context"] = reflect.TypeOf((*context.Context)(nil)).Elem()
	decs["DeadlineExceeded"] = &context.DeadlineExceeded
	decs["TODO"] = context.TODO
	decs["WithCancel"] = context.WithCancel
	decs["WithDeadline"] = context.WithDeadline
	decs["WithTimeout"] = context.WithTimeout
	decs["WithValue"] = context.WithValue
	packages["context"] = native.Package{
		Name:         "context",
		Declarations: decs,
	}
	// "crypto"
	decs = make(native.Declarations, 27)
	decs["BLAKE2b_256"] = crypto.BLAKE2b_256
	decs["BLAKE2b_384"] = crypto.BLAKE2b_384
	decs["BLAKE2b_512"] = crypto.BLAKE2b_512
	decs["BLAKE2s_256"] = crypto.BLAKE2s_256
	decs["Decrypter"] = reflect.TypeOf((*crypto.Decrypter)(nil)).Elem()
	decs["DecrypterOpts"] = reflect.TypeOf((*crypto.DecrypterOpts)(nil)).Elem()
	decs["Hash"] = reflect.TypeOf((*crypto.Hash)(nil)).Elem()
	decs["MD4"] = crypto.MD4
	decs["MD5"] = crypto.MD5
	decs["MD5SHA1"] = crypto.MD5SHA1
	decs["PrivateKey"] = reflect.TypeOf((*crypto.PrivateKey)(nil)).Elem()
	decs["PublicKey"] = reflect.TypeOf((*crypto.PublicKey)(nil)).Elem()
	decs["RIPEMD160"] = crypto.RIPEMD160
	decs["RegisterHash"] = crypto.RegisterHash
	decs["SHA1"] = crypto.SHA1
	decs["SHA224"] = crypto.SHA224
	decs["SHA256"] = crypto.SHA256
	decs["SHA384"] = crypto.SHA384
	decs["SHA3_224"] = crypto.SHA3_224
	decs["SHA3_256"] = crypto.SHA3_256
	decs["SHA3_384"] = crypto.SHA3_384
	decs["SHA3_512"] = crypto.SHA3_512
	decs["SHA512"] = crypto.SHA512
	decs["SHA512_224"] = crypto.SHA512_224
	decs["SHA512_256"] = crypto.SHA512_256
	decs["Signer"] = reflect.TypeOf((*crypto.Signer)(nil)).Elem()
	decs["SignerOpts"] = reflect.TypeOf((*crypto.SignerOpts)(nil)).Elem()
	packages["crypto"] = native.Package{
		Name:         "crypto",
		Declarations: decs,
	}
	// "crypto/aes"
	decs = make(native.Declarations, 3)
	decs["BlockSize"] = native.UntypedNumericConst("16")
	decs["KeySizeError"] = reflect.TypeOf((*aes.KeySizeError)(nil)).Elem()
	decs["NewCipher"] = aes.NewCipher
	packages["crypto/aes"] = native.Package{
		Name:         "aes",
		Declarations: decs,
	}
	// "crypto/cipher"
	decs = make(native.Declarations, 15)
	decs["AEAD"] = reflect.TypeOf((*cipher.AEAD)(nil)).Elem()
	decs["Block"] = reflect.TypeOf((*cipher.Block)(nil)).Elem()
	decs["BlockMode"] = reflect.TypeOf((*cipher.BlockMode)(nil)).Elem()
	decs["NewCBCDecrypter"] = cipher.NewCBCDecrypter
	decs["NewCBCEncrypter"] = cipher.NewCBCEncrypter
	decs["NewCFBDecrypter"] = cipher.NewCFBDecrypter
	decs["NewCFBEncrypter"] = cipher.NewCFBEncrypter
	decs["NewCTR"] = cipher.NewCTR
	decs["NewGCM"] = cipher.NewGCM
	decs["NewGCMWithNonceSize"] = cipher.NewGCMWithNonceSize
	decs["NewGCMWithTagSize"] = cipher.NewGCMWithTagSize
	decs["NewOFB"] = cipher.NewOFB
	decs["Stream"] = reflect.TypeOf((*cipher.Stream)(nil)).Elem()
	decs["StreamReader"] = reflect.TypeOf((*cipher.StreamReader)(nil)).Elem()
	decs["StreamWriter"] = reflect.TypeOf((*cipher.StreamWriter)(nil)).Elem()
	packages["crypto/cipher"] = native.Package{
		Name:         "cipher",
		Declarations: decs,
	}
	// "crypto/des"
	decs = make(native.Declarations, 4)
	decs["BlockSize"] = native.UntypedNumericConst("8")
	decs["KeySizeError"] = reflect.TypeOf((*des.KeySizeError)(nil)).Elem()
	decs["NewCipher"] = des.NewCipher
	decs["NewTripleDESCipher"] = des.NewTripleDESCipher
	packages["crypto/des"] = native.Package{
		Name:         "des",
		Declarations: decs,
	}
	// "crypto/dsa"
	decs = make(native.Declarations, 13)
	decs["ErrInvalidPublicKey"] = &dsa.ErrInvalidPublicKey
	decs["GenerateKey"] = dsa.GenerateKey
	decs["GenerateParameters"] = dsa.GenerateParameters
	decs["L1024N160"] = dsa.L1024N160
	decs["L2048N224"] = dsa.L2048N224
	decs["L2048N256"] = dsa.L2048N256
	decs["L3072N256"] = dsa.L3072N256
	decs["ParameterSizes"] = reflect.TypeOf((*dsa.ParameterSizes)(nil)).Elem()
	decs["Parameters"] = reflect.TypeOf((*dsa.Parameters)(nil)).Elem()
	decs["PrivateKey"] = reflect.TypeOf((*dsa.PrivateKey)(nil)).Elem()
	decs["PublicKey"] = reflect.TypeOf((*dsa.PublicKey)(nil)).Elem()
	decs["Sign"] = dsa.Sign
	decs["Verify"] = dsa.Verify
	packages["crypto/dsa"] = native.Package{
		Name:         "dsa",
		Declarations: decs,
	}
	// "crypto/ecdsa"
	decs = make(native.Declarations, 7)
	decs["GenerateKey"] = ecdsa.GenerateKey
	decs["PrivateKey"] = reflect.TypeOf((*ecdsa.PrivateKey)(nil)).Elem()
	decs["PublicKey"] = reflect.TypeOf((*ecdsa.PublicKey)(nil)).Elem()
	decs["Sign"] = ecdsa.Sign
	decs["SignASN1"] = ecdsa.SignASN1
	decs["Verify"] = ecdsa.Verify
	decs["VerifyASN1"] = ecdsa.VerifyASN1
	packages["crypto/ecdsa"] = native.Package{
		Name:         "ecdsa",
		Declarations: decs,
	}
	// "crypto/elliptic"
	decs = make(native.Declarations, 11)
	decs["Curve"] = reflect.TypeOf((*elliptic.Curve)(nil)).Elem()
	decs["CurveParams"] = reflect.TypeOf((*elliptic.CurveParams)(nil)).Elem()
	decs["GenerateKey"] = elliptic.GenerateKey
	decs["Marshal"] = elliptic.Marshal
	decs["MarshalCompressed"] = elliptic.MarshalCompressed
	decs["P224"] = elliptic.P224
	decs["P256"] = elliptic.P256
	decs["P384"] = elliptic.P384
	decs["P521"] = elliptic.P521
	decs["Unmarshal"] = elliptic.Unmarshal
	decs["UnmarshalCompressed"] = elliptic.UnmarshalCompressed
	packages["crypto/elliptic"] = native.Package{
		Name:         "elliptic",
		Declarations: decs,
	}
	// "crypto/hmac"
	decs = make(native.Declarations, 2)
	decs["Equal"] = hmac.Equal
	decs["New"] = hmac.New
	packages["crypto/hmac"] = native.Package{
		Name:         "hmac",
		Declarations: decs,
	}
	// "crypto/md5"
	decs = make(native.Declarations, 4)
	decs["BlockSize"] = native.UntypedNumericConst("64")
	decs["New"] = md5.New
	decs["Size"] = native.UntypedNumericConst("16")
	decs["Sum"] = md5.Sum
	packages["crypto/md5"] = native.Package{
		Name:         "md5",
		Declarations: decs,
	}
	// "crypto/rand"
	decs = make(native.Declarations, 4)
	decs["Int"] = rand.Int
	decs["Prime"] = rand.Prime
	decs["Read"] = rand.Read
	decs["Reader"] = &rand.Reader
	packages["crypto/rand"] = native.Package{
		Name:         "rand",
		Declarations: decs,
	}
	// "crypto/rc4"
	decs = make(native.Declarations, 3)
	decs["Cipher"] = reflect.TypeOf((*rc4.Cipher)(nil)).Elem()
	decs["KeySizeError"] = reflect.TypeOf((*rc4.KeySizeError)(nil)).Elem()
	decs["NewCipher"] = rc4.NewCipher
	packages["crypto/rc4"] = native.Package{
		Name:         "rc4",
		Declarations: decs,
	}
	// "crypto/rsa"
	decs = make(native.Declarations, 23)
	decs["CRTValue"] = reflect.TypeOf((*rsa.CRTValue)(nil)).Elem()
	decs["DecryptOAEP"] = rsa.DecryptOAEP
	decs["DecryptPKCS1v15"] = rsa.DecryptPKCS1v15
	decs["DecryptPKCS1v15SessionKey"] = rsa.DecryptPKCS1v15SessionKey
	decs["EncryptOAEP"] = rsa.EncryptOAEP
	decs["EncryptPKCS1v15"] = rsa.EncryptPKCS1v15
	decs["ErrDecryption"] = &rsa.ErrDecryption
	decs["ErrMessageTooLong"] = &rsa.ErrMessageTooLong
	decs["ErrVerification"] = &rsa.ErrVerification
	decs["GenerateKey"] = rsa.GenerateKey
	decs["GenerateMultiPrimeKey"] = rsa.GenerateMultiPrimeKey
	decs["OAEPOptions"] = reflect.TypeOf((*rsa.OAEPOptions)(nil)).Elem()
	decs["PKCS1v15DecryptOptions"] = reflect.TypeOf((*rsa.PKCS1v15DecryptOptions)(nil)).Elem()
	decs["PSSOptions"] = reflect.TypeOf((*rsa.PSSOptions)(nil)).Elem()
	decs["PSSSaltLengthAuto"] = native.UntypedNumericConst("0")
	decs["PSSSaltLengthEqualsHash"] = native.UntypedNumericConst("-1")
	decs["PrecomputedValues"] = reflect.TypeOf((*rsa.PrecomputedValues)(nil)).Elem()
	decs["PrivateKey"] = reflect.TypeOf((*rsa.PrivateKey)(nil)).Elem()
	decs["PublicKey"] = reflect.TypeOf((*rsa.PublicKey)(nil)).Elem()
	decs["SignPKCS1v15"] = rsa.SignPKCS1v15
	decs["SignPSS"] = rsa.SignPSS
	decs["VerifyPKCS1v15"] = rsa.VerifyPKCS1v15
	decs["VerifyPSS"] = rsa.VerifyPSS
	packages["crypto/rsa"] = native.Package{
		Name:         "rsa",
		Declarations: decs,
	}
	// "crypto/sha1"
	decs = make(native.Declarations, 4)
	decs["BlockSize"] = native.UntypedNumericConst("64")
	decs["New"] = sha1.New
	decs["Size"] = native.UntypedNumericConst("20")
	decs["Sum"] = sha1.Sum
	packages["crypto/sha1"] = native.Package{
		Name:         "sha1",
		Declarations: decs,
	}
	// "crypto/sha256"
	decs = make(native.Declarations, 7)
	decs["BlockSize"] = native.UntypedNumericConst("64")
	decs["New"] = sha256.New
	decs["New224"] = sha256.New224
	decs["Size"] = native.UntypedNumericConst("32")
	decs["Size224"] = native.UntypedNumericConst("28")
	decs["Sum224"] = sha256.Sum224
	decs["Sum256"] = sha256.Sum256
	packages["crypto/sha256"] = native.Package{
		Name:         "sha256",
		Declarations: decs,
	}
	// "crypto/sha512"
	decs = make(native.Declarations, 13)
	decs["BlockSize"] = native.UntypedNumericConst("128")
	decs["New"] = sha512.New
	decs["New384"] = sha512.New384
	decs["New512_224"] = sha512.New512_224
	decs["New512_256"] = sha512.New512_256
	decs["Size"] = native.UntypedNumericConst("64")
	decs["Size224"] = native.UntypedNumericConst("28")
	decs["Size256"] = native.UntypedNumericConst("32")
	decs["Size384"] = native.UntypedNumericConst("48")
	decs["Sum384"] = sha512.Sum384
	decs["Sum512"] = sha512.Sum512
	decs["Sum512_224"] = sha512.Sum512_224
	decs["Sum512_256"] = sha512.Sum512_256
	packages["crypto/sha512"] = native.Package{
		Name:         "sha512",
		Declarations: decs,
	}
	// "crypto/subtle"
	decs = make(native.Declarations, 6)
	decs["ConstantTimeByteEq"] = subtle.ConstantTimeByteEq
	decs["ConstantTimeCompare"] = subtle.ConstantTimeCompare
	decs["ConstantTimeCopy"] = subtle.ConstantTimeCopy
	decs["ConstantTimeEq"] = subtle.ConstantTimeEq
	decs["ConstantTimeLessOrEq"] = subtle.ConstantTimeLessOrEq
	decs["ConstantTimeSelect"] = subtle.ConstantTimeSelect
	packages["crypto/subtle"] = native.Package{
		Name:         "subtle",
		Declarations: decs,
	}
	// "crypto/tls"
	decs = make(native.Declarations, 84)
	decs["Certificate"] = reflect.TypeOf((*tls.Certificate)(nil)).Elem()
	decs["CertificateRequestInfo"] = reflect.TypeOf((*tls.CertificateRequestInfo)(nil)).Elem()
	decs["CipherSuite"] = reflect.TypeOf((*tls.CipherSuite)(nil)).Elem()
	decs["CipherSuiteName"] = tls.CipherSuiteName
	decs["CipherSuites"] = tls.CipherSuites
	decs["Client"] = tls.Client
	decs["ClientAuthType"] = reflect.TypeOf((*tls.ClientAuthType)(nil)).Elem()
	decs["ClientHelloInfo"] = reflect.TypeOf((*tls.ClientHelloInfo)(nil)).Elem()
	decs["ClientSessionCache"] = reflect.TypeOf((*tls.ClientSessionCache)(nil)).Elem()
	decs["ClientSessionState"] = reflect.TypeOf((*tls.ClientSessionState)(nil)).Elem()
	decs["Config"] = reflect.TypeOf((*tls.Config)(nil)).Elem()
	decs["Conn"] = reflect.TypeOf((*tls.Conn)(nil)).Elem()
	decs["ConnectionState"] = reflect.TypeOf((*tls.ConnectionState)(nil)).Elem()
	decs["CurveID"] = reflect.TypeOf((*tls.CurveID)(nil)).Elem()
	decs["CurveP256"] = tls.CurveP256
	decs["CurveP384"] = tls.CurveP384
	decs["CurveP521"] = tls.CurveP521
	decs["Dial"] = tls.Dial
	decs["DialWithDialer"] = tls.DialWithDialer
	decs["Dialer"] = reflect.TypeOf((*tls.Dialer)(nil)).Elem()
	decs["ECDSAWithP256AndSHA256"] = tls.ECDSAWithP256AndSHA256
	decs["ECDSAWithP384AndSHA384"] = tls.ECDSAWithP384AndSHA384
	decs["ECDSAWithP521AndSHA512"] = tls.ECDSAWithP521AndSHA512
	decs["ECDSAWithSHA1"] = tls.ECDSAWithSHA1
	decs["Ed25519"] = tls.Ed25519
	decs["InsecureCipherSuites"] = tls.InsecureCipherSuites
	decs["Listen"] = tls.Listen
	decs["LoadX509KeyPair"] = tls.LoadX509KeyPair
	decs["NewLRUClientSessionCache"] = tls.NewLRUClientSessionCache
	decs["NewListener"] = tls.NewListener
	decs["NoClientCert"] = tls.NoClientCert
	decs["PKCS1WithSHA1"] = tls.PKCS1WithSHA1
	decs["PKCS1WithSHA256"] = tls.PKCS1WithSHA256
	decs["PKCS1WithSHA384"] = tls.PKCS1WithSHA384
	decs["PKCS1WithSHA512"] = tls.PKCS1WithSHA512
	decs["PSSWithSHA256"] = tls.PSSWithSHA256
	decs["PSSWithSHA384"] = tls.PSSWithSHA384
	decs["PSSWithSHA512"] = tls.PSSWithSHA512
	decs["RecordHeaderError"] = reflect.TypeOf((*tls.RecordHeaderError)(nil)).Elem()
	decs["RenegotiateFreelyAsClient"] = tls.RenegotiateFreelyAsClient
	decs["RenegotiateNever"] = tls.RenegotiateNever
	decs["RenegotiateOnceAsClient"] = tls.RenegotiateOnceAsClient
	decs["RenegotiationSupport"] = reflect.TypeOf((*tls.RenegotiationSupport)(nil)).Elem()
	decs["RequestClientCert"] = tls.RequestClientCert
	decs["RequireAndVerifyClientCert"] = tls.RequireAndVerifyClientCert
	decs["RequireAnyClientCert"] = tls.RequireAnyClientCert
	decs["Server"] = tls.Server
	decs["SignatureScheme"] = reflect.TypeOf((*tls.SignatureScheme)(nil)).Elem()
	decs["TLS_AES_128_GCM_SHA256"] = tls.TLS_AES_128_GCM_SHA256
	decs["TLS_AES_256_GCM_SHA384"] = tls.TLS_AES_256_GCM_SHA384
	decs["TLS_CHACHA20_POLY1305_SHA256"] = tls.TLS_CHACHA20_POLY1305_SHA256
	decs["TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"] = tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA
	decs["TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"] = tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256
	decs["TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"] = tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
	decs["TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"] = tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA
	decs["TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"] = tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
	decs["TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305"] = tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305
	decs["TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"] = tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
	decs["TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"] = tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA
	decs["TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"] = tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA
	decs["TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"] = tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
	decs["TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"] = tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256
	decs["TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"] = tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
	decs["TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"] = tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
	decs["TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"] = tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
	decs["TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305"] = tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305
	decs["TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"] = tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
	decs["TLS_ECDHE_RSA_WITH_RC4_128_SHA"] = tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA
	decs["TLS_FALLBACK_SCSV"] = tls.TLS_FALLBACK_SCSV
	decs["TLS_RSA_WITH_3DES_EDE_CBC_SHA"] = tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA
	decs["TLS_RSA_WITH_AES_128_CBC_SHA"] = tls.TLS_RSA_WITH_AES_128_CBC_SHA
	decs["TLS_RSA_WITH_AES_128_CBC_SHA256"] = tls.TLS_RSA_WITH_AES_128_CBC_SHA256
	decs["TLS_RSA_WITH_AES_128_GCM_SHA256"] = tls.TLS_RSA_WITH_AES_128_GCM_SHA256
	decs["TLS_RSA_WITH_AES_256_CBC_SHA"] = tls.TLS_RSA_WITH_AES_256_CBC_SHA
	decs["TLS_RSA_WITH_AES_256_GCM_SHA384"] = tls.TLS_RSA_WITH_AES_256_GCM_SHA384
	decs["TLS_RSA_WITH_RC4_128_SHA"] = tls.TLS_RSA_WITH_RC4_128_SHA
	decs["VerifyClientCertIfGiven"] = tls.VerifyClientCertIfGiven
	decs["VersionSSL30"] = native.UntypedNumericConst("768")
	decs["VersionTLS10"] = native.UntypedNumericConst("769")
	decs["VersionTLS11"] = native.UntypedNumericConst("770")
	decs["VersionTLS12"] = native.UntypedNumericConst("771")
	decs["VersionTLS13"] = native.UntypedNumericConst("772")
	decs["X25519"] = tls.X25519
	decs["X509KeyPair"] = tls.X509KeyPair
	packages["crypto/tls"] = native.Package{
		Name:         "tls",
		Declarations: decs,
	}
	// "crypto/x509"
	decs = make(native.Declarations, 103)
	decs["CANotAuthorizedForExtKeyUsage"] = x509.CANotAuthorizedForExtKeyUsage
	decs["CANotAuthorizedForThisName"] = x509.CANotAuthorizedForThisName
	decs["CertPool"] = reflect.TypeOf((*x509.CertPool)(nil)).Elem()
	decs["Certificate"] = reflect.TypeOf((*x509.Certificate)(nil)).Elem()
	decs["CertificateInvalidError"] = reflect.TypeOf((*x509.CertificateInvalidError)(nil)).Elem()
	decs["CertificateRequest"] = reflect.TypeOf((*x509.CertificateRequest)(nil)).Elem()
	decs["ConstraintViolationError"] = reflect.TypeOf((*x509.ConstraintViolationError)(nil)).Elem()
	decs["CreateCertificate"] = x509.CreateCertificate
	decs["CreateCertificateRequest"] = x509.CreateCertificateRequest
	decs["CreateRevocationList"] = x509.CreateRevocationList
	decs["DSA"] = x509.DSA
	decs["DSAWithSHA1"] = x509.DSAWithSHA1
	decs["DSAWithSHA256"] = x509.DSAWithSHA256
	decs["DecryptPEMBlock"] = x509.DecryptPEMBlock
	decs["ECDSA"] = x509.ECDSA
	decs["ECDSAWithSHA1"] = x509.ECDSAWithSHA1
	decs["ECDSAWithSHA256"] = x509.ECDSAWithSHA256
	decs["ECDSAWithSHA384"] = x509.ECDSAWithSHA384
	decs["ECDSAWithSHA512"] = x509.ECDSAWithSHA512
	decs["Ed25519"] = x509.Ed25519
	decs["EncryptPEMBlock"] = x509.EncryptPEMBlock
	decs["ErrUnsupportedAlgorithm"] = &x509.ErrUnsupportedAlgorithm
	decs["Expired"] = x509.Expired
	decs["ExtKeyUsage"] = reflect.TypeOf((*x509.ExtKeyUsage)(nil)).Elem()
	decs["ExtKeyUsageAny"] = x509.ExtKeyUsageAny
	decs["ExtKeyUsageClientAuth"] = x509.ExtKeyUsageClientAuth
	decs["ExtKeyUsageCodeSigning"] = x509.ExtKeyUsageCodeSigning
	decs["ExtKeyUsageEmailProtection"] = x509.ExtKeyUsageEmailProtection
	decs["ExtKeyUsageIPSECEndSystem"] = x509.ExtKeyUsageIPSECEndSystem
	decs["ExtKeyUsageIPSECTunnel"] = x509.ExtKeyUsageIPSECTunnel
	decs["ExtKeyUsageIPSECUser"] = x509.ExtKeyUsageIPSECUser
	decs["ExtKeyUsageMicrosoftCommercialCodeSigning"] = x509.ExtKeyUsageMicrosoftCommercialCodeSigning
	decs["ExtKeyUsageMicrosoftKernelCodeSigning"] = x509.ExtKeyUsageMicrosoftKernelCodeSigning
	decs["ExtKeyUsageMicrosoftServerGatedCrypto"] = x509.ExtKeyUsageMicrosoftServerGatedCrypto
	decs["ExtKeyUsageNetscapeServerGatedCrypto"] = x509.ExtKeyUsageNetscapeServerGatedCrypto
	decs["ExtKeyUsageOCSPSigning"] = x509.ExtKeyUsageOCSPSigning
	decs["ExtKeyUsageServerAuth"] = x509.ExtKeyUsageServerAuth
	decs["ExtKeyUsageTimeStamping"] = x509.ExtKeyUsageTimeStamping
	decs["HostnameError"] = reflect.TypeOf((*x509.HostnameError)(nil)).Elem()
	decs["IncompatibleUsage"] = x509.IncompatibleUsage
	decs["IncorrectPasswordError"] = &x509.IncorrectPasswordError
	decs["InsecureAlgorithmError"] = reflect.TypeOf((*x509.InsecureAlgorithmError)(nil)).Elem()
	decs["InvalidReason"] = reflect.TypeOf((*x509.InvalidReason)(nil)).Elem()
	decs["IsEncryptedPEMBlock"] = x509.IsEncryptedPEMBlock
	decs["KeyUsage"] = reflect.TypeOf((*x509.KeyUsage)(nil)).Elem()
	decs["KeyUsageCRLSign"] = x509.KeyUsageCRLSign
	decs["KeyUsageCertSign"] = x509.KeyUsageCertSign
	decs["KeyUsageContentCommitment"] = x509.KeyUsageContentCommitment
	decs["KeyUsageDataEncipherment"] = x509.KeyUsageDataEncipherment
	decs["KeyUsageDecipherOnly"] = x509.KeyUsageDecipherOnly
	decs["KeyUsageDigitalSignature"] = x509.KeyUsageDigitalSignature
	decs["KeyUsageEncipherOnly"] = x509.KeyUsageEncipherOnly
	decs["KeyUsageKeyAgreement"] = x509.KeyUsageKeyAgreement
	decs["KeyUsageKeyEncipherment"] = x509.KeyUsageKeyEncipherment
	decs["MD2WithRSA"] = x509.MD2WithRSA
	decs["MD5WithRSA"] = x509.MD5WithRSA
	decs["MarshalECPrivateKey"] = x509.MarshalECPrivateKey
	decs["MarshalPKCS1PrivateKey"] = x509.MarshalPKCS1PrivateKey
	decs["MarshalPKCS1PublicKey"] = x509.MarshalPKCS1PublicKey
	decs["MarshalPKCS8PrivateKey"] = x509.MarshalPKCS8PrivateKey
	decs["MarshalPKIXPublicKey"] = x509.MarshalPKIXPublicKey
	decs["NameConstraintsWithoutSANs"] = x509.NameConstraintsWithoutSANs
	decs["NameMismatch"] = x509.NameMismatch
	decs["NewCertPool"] = x509.NewCertPool
	decs["NotAuthorizedToSign"] = x509.NotAuthorizedToSign
	decs["PEMCipher"] = reflect.TypeOf((*x509.PEMCipher)(nil)).Elem()
	decs["PEMCipher3DES"] = x509.PEMCipher3DES
	decs["PEMCipherAES128"] = x509.PEMCipherAES128
	decs["PEMCipherAES192"] = x509.PEMCipherAES192
	decs["PEMCipherAES256"] = x509.PEMCipherAES256
	decs["PEMCipherDES"] = x509.PEMCipherDES
	decs["ParseCRL"] = x509.ParseCRL
	decs["ParseCertificate"] = x509.ParseCertificate
	decs["ParseCertificateRequest"] = x509.ParseCertificateRequest
	decs["ParseCertificates"] = x509.ParseCertificates
	decs["ParseDERCRL"] = x509.ParseDERCRL
	decs["ParseECPrivateKey"] = x509.ParseECPrivateKey
	decs["ParsePKCS1PrivateKey"] = x509.ParsePKCS1PrivateKey
	decs["ParsePKCS1PublicKey"] = x509.ParsePKCS1PublicKey
	decs["ParsePKCS8PrivateKey"] = x509.ParsePKCS8PrivateKey
	decs["ParsePKIXPublicKey"] = x509.ParsePKIXPublicKey
	decs["PublicKeyAlgorithm"] = reflect.TypeOf((*x509.PublicKeyAlgorithm)(nil)).Elem()
	decs["PureEd25519"] = x509.PureEd25519
	decs["RSA"] = x509.RSA
	decs["RevocationList"] = reflect.TypeOf((*x509.RevocationList)(nil)).Elem()
	decs["SHA1WithRSA"] = x509.SHA1WithRSA
	decs["SHA256WithRSA"] = x509.SHA256WithRSA
	decs["SHA256WithRSAPSS"] = x509.SHA256WithRSAPSS
	decs["SHA384WithRSA"] = x509.SHA384WithRSA
	decs["SHA384WithRSAPSS"] = x509.SHA384WithRSAPSS
	decs["SHA512WithRSA"] = x509.SHA512WithRSA
	decs["SHA512WithRSAPSS"] = x509.SHA512WithRSAPSS
	decs["SignatureAlgorithm"] = reflect.TypeOf((*x509.SignatureAlgorithm)(nil)).Elem()
	decs["SystemCertPool"] = x509.SystemCertPool
	decs["SystemRootsError"] = reflect.TypeOf((*x509.SystemRootsError)(nil)).Elem()
	decs["TooManyConstraints"] = x509.TooManyConstraints
	decs["TooManyIntermediates"] = x509.TooManyIntermediates
	decs["UnconstrainedName"] = x509.UnconstrainedName
	decs["UnhandledCriticalExtension"] = reflect.TypeOf((*x509.UnhandledCriticalExtension)(nil)).Elem()
	decs["UnknownAuthorityError"] = reflect.TypeOf((*x509.UnknownAuthorityError)(nil)).Elem()
	decs["UnknownPublicKeyAlgorithm"] = x509.UnknownPublicKeyAlgorithm
	decs["UnknownSignatureAlgorithm"] = x509.UnknownSignatureAlgorithm
	decs["VerifyOptions"] = reflect.TypeOf((*x509.VerifyOptions)(nil)).Elem()
	packages["crypto/x509"] = native.Package{
		Name:         "x509",
		Declarations: decs,
	}
	// "crypto/x509/pkix"
	decs = make(native.Declarations, 10)
	decs["AlgorithmIdentifier"] = reflect.TypeOf((*pkix.AlgorithmIdentifier)(nil)).Elem()
	decs["AttributeTypeAndValue"] = reflect.TypeOf((*pkix.AttributeTypeAndValue)(nil)).Elem()
	decs["AttributeTypeAndValueSET"] = reflect.TypeOf((*pkix.AttributeTypeAndValueSET)(nil)).Elem()
	decs["CertificateList"] = reflect.TypeOf((*pkix.CertificateList)(nil)).Elem()
	decs["Extension"] = reflect.TypeOf((*pkix.Extension)(nil)).Elem()
	decs["Name"] = reflect.TypeOf((*pkix.Name)(nil)).Elem()
	decs["RDNSequence"] = reflect.TypeOf((*pkix.RDNSequence)(nil)).Elem()
	decs["RelativeDistinguishedNameSET"] = reflect.TypeOf((*pkix.RelativeDistinguishedNameSET)(nil)).Elem()
	decs["RevokedCertificate"] = reflect.TypeOf((*pkix.RevokedCertificate)(nil)).Elem()
	decs["TBSCertificateList"] = reflect.TypeOf((*pkix.TBSCertificateList)(nil)).Elem()
	packages["crypto/x509/pkix"] = native.Package{
		Name:         "pkix",
		Declarations: decs,
	}
	// "encoding"
	decs = make(native.Declarations, 4)
	decs["BinaryMarshaler"] = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	decs["BinaryUnmarshaler"] = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()
	decs["TextMarshaler"] = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	decs["TextUnmarshaler"] = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	packages["encoding"] = native.Package{
		Name:         "encoding",
		Declarations: decs,
	}
	// "encoding/ascii85"
	decs = make(native.Declarations, 6)
	decs["CorruptInputError"] = reflect.TypeOf((*ascii85.CorruptInputError)(nil)).Elem()
	decs["Decode"] = ascii85.Decode
	decs["Encode"] = ascii85.Encode
	decs["MaxEncodedLen"] = ascii85.MaxEncodedLen
	decs["NewDecoder"] = ascii85.NewDecoder
	decs["NewEncoder"] = ascii85.NewEncoder
	packages["encoding/ascii85"] = native.Package{
		Name:         "ascii85",
		Declarations: decs,
	}
	// "encoding/asn1"
	decs = make(native.Declarations, 36)
	decs["BitString"] = reflect.TypeOf((*asn1.BitString)(nil)).Elem()
	decs["ClassApplication"] = native.UntypedNumericConst("1")
	decs["ClassContextSpecific"] = native.UntypedNumericConst("2")
	decs["ClassPrivate"] = native.UntypedNumericConst("3")
	decs["ClassUniversal"] = native.UntypedNumericConst("0")
	decs["Enumerated"] = reflect.TypeOf((*asn1.Enumerated)(nil)).Elem()
	decs["Flag"] = reflect.TypeOf((*asn1.Flag)(nil)).Elem()
	decs["Marshal"] = asn1.Marshal
	decs["MarshalWithParams"] = asn1.MarshalWithParams
	decs["NullBytes"] = &asn1.NullBytes
	decs["NullRawValue"] = &asn1.NullRawValue
	decs["ObjectIdentifier"] = reflect.TypeOf((*asn1.ObjectIdentifier)(nil)).Elem()
	decs["RawContent"] = reflect.TypeOf((*asn1.RawContent)(nil)).Elem()
	decs["RawValue"] = reflect.TypeOf((*asn1.RawValue)(nil)).Elem()
	decs["StructuralError"] = reflect.TypeOf((*asn1.StructuralError)(nil)).Elem()
	decs["SyntaxError"] = reflect.TypeOf((*asn1.SyntaxError)(nil)).Elem()
	decs["TagBMPString"] = native.UntypedNumericConst("30")
	decs["TagBitString"] = native.UntypedNumericConst("3")
	decs["TagBoolean"] = native.UntypedNumericConst("1")
	decs["TagEnum"] = native.UntypedNumericConst("10")
	decs["TagGeneralString"] = native.UntypedNumericConst("27")
	decs["TagGeneralizedTime"] = native.UntypedNumericConst("24")
	decs["TagIA5String"] = native.UntypedNumericConst("22")
	decs["TagInteger"] = native.UntypedNumericConst("2")
	decs["TagNull"] = native.UntypedNumericConst("5")
	decs["TagNumericString"] = native.UntypedNumericConst("18")
	decs["TagOID"] = native.UntypedNumericConst("6")
	decs["TagOctetString"] = native.UntypedNumericConst("4")
	decs["TagPrintableString"] = native.UntypedNumericConst("19")
	decs["TagSequence"] = native.UntypedNumericConst("16")
	decs["TagSet"] = native.UntypedNumericConst("17")
	decs["TagT61String"] = native.UntypedNumericConst("20")
	decs["TagUTCTime"] = native.UntypedNumericConst("23")
	decs["TagUTF8String"] = native.UntypedNumericConst("12")
	decs["Unmarshal"] = asn1.Unmarshal
	decs["UnmarshalWithParams"] = asn1.UnmarshalWithParams
	packages["encoding/asn1"] = native.Package{
		Name:         "asn1",
		Declarations: decs,
	}
	// "encoding/base32"
	decs = make(native.Declarations, 9)
	decs["CorruptInputError"] = reflect.TypeOf((*base32.CorruptInputError)(nil)).Elem()
	decs["Encoding"] = reflect.TypeOf((*base32.Encoding)(nil)).Elem()
	decs["HexEncoding"] = &base32.HexEncoding
	decs["NewDecoder"] = base32.NewDecoder
	decs["NewEncoder"] = base32.NewEncoder
	decs["NewEncoding"] = base32.NewEncoding
	decs["NoPadding"] = base32.NoPadding
	decs["StdEncoding"] = &base32.StdEncoding
	decs["StdPadding"] = base32.StdPadding
	packages["encoding/base32"] = native.Package{
		Name:         "base32",
		Declarations: decs,
	}
	// "encoding/base64"
	decs = make(native.Declarations, 11)
	decs["CorruptInputError"] = reflect.TypeOf((*base64.CorruptInputError)(nil)).Elem()
	decs["Encoding"] = reflect.TypeOf((*base64.Encoding)(nil)).Elem()
	decs["NewDecoder"] = base64.NewDecoder
	decs["NewEncoder"] = base64.NewEncoder
	decs["NewEncoding"] = base64.NewEncoding
	decs["NoPadding"] = base64.NoPadding
	decs["RawStdEncoding"] = &base64.RawStdEncoding
	decs["RawURLEncoding"] = &base64.RawURLEncoding
	decs["StdEncoding"] = &base64.StdEncoding
	decs["StdPadding"] = base64.StdPadding
	decs["URLEncoding"] = &base64.URLEncoding
	packages["encoding/base64"] = native.Package{
		Name:         "base64",
		Declarations: decs,
	}
	// "encoding/binary"
	decs = make(native.Declarations, 15)
	decs["BigEndian"] = &binary.BigEndian
	decs["ByteOrder"] = reflect.TypeOf((*binary.ByteOrder)(nil)).Elem()
	decs["LittleEndian"] = &binary.LittleEndian
	decs["MaxVarintLen16"] = native.UntypedNumericConst("3")
	decs["MaxVarintLen32"] = native.UntypedNumericConst("5")
	decs["MaxVarintLen64"] = native.UntypedNumericConst("10")
	decs["PutUvarint"] = binary.PutUvarint
	decs["PutVarint"] = binary.PutVarint
	decs["Read"] = binary.Read
	decs["ReadUvarint"] = binary.ReadUvarint
	decs["ReadVarint"] = binary.ReadVarint
	decs["Size"] = binary.Size
	decs["Uvarint"] = binary.Uvarint
	decs["Varint"] = binary.Varint
	decs["Write"] = binary.Write
	packages["encoding/binary"] = native.Package{
		Name:         "binary",
		Declarations: decs,
	}
	// "encoding/csv"
	decs = make(native.Declarations, 9)
	decs["ErrBareQuote"] = &csv.ErrBareQuote
	decs["ErrFieldCount"] = &csv.ErrFieldCount
	decs["ErrQuote"] = &csv.ErrQuote
	decs["ErrTrailingComma"] = &csv.ErrTrailingComma
	decs["NewReader"] = csv.NewReader
	decs["NewWriter"] = csv.NewWriter
	decs["ParseError"] = reflect.TypeOf((*csv.ParseError)(nil)).Elem()
	decs["Reader"] = reflect.TypeOf((*csv.Reader)(nil)).Elem()
	decs["Writer"] = reflect.TypeOf((*csv.Writer)(nil)).Elem()
	packages["encoding/csv"] = native.Package{
		Name:         "csv",
		Declarations: decs,
	}
	// "encoding/gob"
	decs = make(native.Declarations, 9)
	decs["CommonType"] = reflect.TypeOf((*gob.CommonType)(nil)).Elem()
	decs["Decoder"] = reflect.TypeOf((*gob.Decoder)(nil)).Elem()
	decs["Encoder"] = reflect.TypeOf((*gob.Encoder)(nil)).Elem()
	decs["GobDecoder"] = reflect.TypeOf((*gob.GobDecoder)(nil)).Elem()
	decs["GobEncoder"] = reflect.TypeOf((*gob.GobEncoder)(nil)).Elem()
	decs["NewDecoder"] = gob.NewDecoder
	decs["NewEncoder"] = gob.NewEncoder
	decs["Register"] = gob.Register
	decs["RegisterName"] = gob.RegisterName
	packages["encoding/gob"] = native.Package{
		Name:         "gob",
		Declarations: decs,
	}
	// "encoding/hex"
	decs = make(native.Declarations, 12)
	decs["Decode"] = hex.Decode
	decs["DecodeString"] = hex.DecodeString
	decs["DecodedLen"] = hex.DecodedLen
	decs["Dump"] = hex.Dump
	decs["Dumper"] = hex.Dumper
	decs["Encode"] = hex.Encode
	decs["EncodeToString"] = hex.EncodeToString
	decs["EncodedLen"] = hex.EncodedLen
	decs["ErrLength"] = &hex.ErrLength
	decs["InvalidByteError"] = reflect.TypeOf((*hex.InvalidByteError)(nil)).Elem()
	decs["NewDecoder"] = hex.NewDecoder
	decs["NewEncoder"] = hex.NewEncoder
	packages["encoding/hex"] = native.Package{
		Name:         "hex",
		Declarations: decs,
	}
	// "encoding/json"
	decs = make(native.Declarations, 25)
	decs["Compact"] = json.Compact
	decs["Decoder"] = reflect.TypeOf((*json.Decoder)(nil)).Elem()
	decs["Delim"] = reflect.TypeOf((*json.Delim)(nil)).Elem()
	decs["Encoder"] = reflect.TypeOf((*json.Encoder)(nil)).Elem()
	decs["HTMLEscape"] = json.HTMLEscape
	decs["Indent"] = json.Indent
	decs["InvalidUTF8Error"] = reflect.TypeOf((*json.InvalidUTF8Error)(nil)).Elem()
	decs["InvalidUnmarshalError"] = reflect.TypeOf((*json.InvalidUnmarshalError)(nil)).Elem()
	decs["Marshal"] = json.Marshal
	decs["MarshalIndent"] = json.MarshalIndent
	decs["Marshaler"] = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	decs["MarshalerError"] = reflect.TypeOf((*json.MarshalerError)(nil)).Elem()
	decs["NewDecoder"] = json.NewDecoder
	decs["NewEncoder"] = json.NewEncoder
	decs["Number"] = reflect.TypeOf((*json.Number)(nil)).Elem()
	decs["RawMessage"] = reflect.TypeOf((*json.RawMessage)(nil)).Elem()
	decs["SyntaxError"] = reflect.TypeOf((*json.SyntaxError)(nil)).Elem()
	decs["Token"] = reflect.TypeOf((*json.Token)(nil)).Elem()
	decs["Unmarshal"] = json.Unmarshal
	decs["UnmarshalFieldError"] = reflect.TypeOf((*json.UnmarshalFieldError)(nil)).Elem()
	decs["UnmarshalTypeError"] = reflect.TypeOf((*json.UnmarshalTypeError)(nil)).Elem()
	decs["Unmarshaler"] = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	decs["UnsupportedTypeError"] = reflect.TypeOf((*json.UnsupportedTypeError)(nil)).Elem()
	decs["UnsupportedValueError"] = reflect.TypeOf((*json.UnsupportedValueError)(nil)).Elem()
	decs["Valid"] = json.Valid
	packages["encoding/json"] = native.Package{
		Name:         "json",
		Declarations: decs,
	}
	// "encoding/pem"
	decs = make(native.Declarations, 4)
	decs["Block"] = reflect.TypeOf((*pem.Block)(nil)).Elem()
	decs["Decode"] = pem.Decode
	decs["Encode"] = pem.Encode
	decs["EncodeToMemory"] = pem.EncodeToMemory
	packages["encoding/pem"] = native.Package{
		Name:         "pem",
		Declarations: decs,
	}
	// "encoding/xml"
	decs = make(native.Declarations, 32)
	decs["Attr"] = reflect.TypeOf((*xml.Attr)(nil)).Elem()
	decs["CharData"] = reflect.TypeOf((*xml.CharData)(nil)).Elem()
	decs["Comment"] = reflect.TypeOf((*xml.Comment)(nil)).Elem()
	decs["CopyToken"] = xml.CopyToken
	decs["Decoder"] = reflect.TypeOf((*xml.Decoder)(nil)).Elem()
	decs["Directive"] = reflect.TypeOf((*xml.Directive)(nil)).Elem()
	decs["Encoder"] = reflect.TypeOf((*xml.Encoder)(nil)).Elem()
	decs["EndElement"] = reflect.TypeOf((*xml.EndElement)(nil)).Elem()
	decs["Escape"] = xml.Escape
	decs["EscapeText"] = xml.EscapeText
	decs["HTMLAutoClose"] = &xml.HTMLAutoClose
	decs["HTMLEntity"] = &xml.HTMLEntity
	decs["Header"] = native.UntypedStringConst("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	decs["Marshal"] = xml.Marshal
	decs["MarshalIndent"] = xml.MarshalIndent
	decs["Marshaler"] = reflect.TypeOf((*xml.Marshaler)(nil)).Elem()
	decs["MarshalerAttr"] = reflect.TypeOf((*xml.MarshalerAttr)(nil)).Elem()
	decs["Name"] = reflect.TypeOf((*xml.Name)(nil)).Elem()
	decs["NewDecoder"] = xml.NewDecoder
	decs["NewEncoder"] = xml.NewEncoder
	decs["NewTokenDecoder"] = xml.NewTokenDecoder
	decs["ProcInst"] = reflect.TypeOf((*xml.ProcInst)(nil)).Elem()
	decs["StartElement"] = reflect.TypeOf((*xml.StartElement)(nil)).Elem()
	decs["SyntaxError"] = reflect.TypeOf((*xml.SyntaxError)(nil)).Elem()
	decs["TagPathError"] = reflect.TypeOf((*xml.TagPathError)(nil)).Elem()
	decs["Token"] = reflect.TypeOf((*xml.Token)(nil)).Elem()
	decs["TokenReader"] = reflect.TypeOf((*xml.TokenReader)(nil)).Elem()
	decs["Unmarshal"] = xml.Unmarshal
	decs["UnmarshalError"] = reflect.TypeOf((*xml.UnmarshalError)(nil)).Elem()
	decs["Unmarshaler"] = reflect.TypeOf((*xml.Unmarshaler)(nil)).Elem()
	decs["UnmarshalerAttr"] = reflect.TypeOf((*xml.UnmarshalerAttr)(nil)).Elem()
	decs["UnsupportedTypeError"] = reflect.TypeOf((*xml.UnsupportedTypeError)(nil)).Elem()
	packages["encoding/xml"] = native.Package{
		Name:         "xml",
		Declarations: decs,
	}
	// "errors"
	decs = make(native.Declarations, 4)
	decs["As"] = errors.As
	decs["Is"] = errors.Is
	decs["New"] = errors.New
	decs["Unwrap"] = errors.Unwrap
	packages["errors"] = native.Package{
		Name:         "errors",
		Declarations: decs,
	}
	// "expvar"
	decs = make(native.Declarations, 15)
	decs["Do"] = expvar.Do
	decs["Float"] = reflect.TypeOf((*expvar.Float)(nil)).Elem()
	decs["Func"] = reflect.TypeOf((*expvar.Func)(nil)).Elem()
	decs["Get"] = expvar.Get
	decs["Handler"] = expvar.Handler
	decs["Int"] = reflect.TypeOf((*expvar.Int)(nil)).Elem()
	decs["KeyValue"] = reflect.TypeOf((*expvar.KeyValue)(nil)).Elem()
	decs["Map"] = reflect.TypeOf((*expvar.Map)(nil)).Elem()
	decs["NewFloat"] = expvar.NewFloat
	decs["NewInt"] = expvar.NewInt
	decs["NewMap"] = expvar.NewMap
	decs["NewString"] = expvar.NewString
	decs["Publish"] = expvar.Publish
	decs["String"] = reflect.TypeOf((*expvar.String)(nil)).Elem()
	decs["Var"] = reflect.TypeOf((*expvar.Var)(nil)).Elem()
	packages["expvar"] = native.Package{
		Name:         "expvar",
		Declarations: decs,
	}
	// "flag"
	decs = make(native.Declarations, 42)
	decs["Arg"] = flag.Arg
	decs["Args"] = flag.Args
	decs["Bool"] = flag.Bool
	decs["BoolVar"] = flag.BoolVar
	decs["CommandLine"] = &flag.CommandLine
	decs["ContinueOnError"] = flag.ContinueOnError
	decs["Duration"] = flag.Duration
	decs["DurationVar"] = flag.DurationVar
	decs["ErrHelp"] = &flag.ErrHelp
	decs["ErrorHandling"] = reflect.TypeOf((*flag.ErrorHandling)(nil)).Elem()
	decs["ExitOnError"] = flag.ExitOnError
	decs["Flag"] = reflect.TypeOf((*flag.Flag)(nil)).Elem()
	decs["FlagSet"] = reflect.TypeOf((*flag.FlagSet)(nil)).Elem()
	decs["Float64"] = flag.Float64
	decs["Float64Var"] = flag.Float64Var
	decs["Func"] = flag.Func
	decs["Getter"] = reflect.TypeOf((*flag.Getter)(nil)).Elem()
	decs["Int"] = flag.Int
	decs["Int64"] = flag.Int64
	decs["Int64Var"] = flag.Int64Var
	decs["IntVar"] = flag.IntVar
	decs["Lookup"] = flag.Lookup
	decs["NArg"] = flag.NArg
	decs["NFlag"] = flag.NFlag
	decs["NewFlagSet"] = flag.NewFlagSet
	decs["PanicOnError"] = flag.PanicOnError
	decs["Parse"] = flag.Parse
	decs["Parsed"] = flag.Parsed
	decs["PrintDefaults"] = flag.PrintDefaults
	decs["Set"] = flag.Set
	decs["String"] = flag.String
	decs["StringVar"] = flag.StringVar
	decs["Uint"] = flag.Uint
	decs["Uint64"] = flag.Uint64
	decs["Uint64Var"] = flag.Uint64Var
	decs["UintVar"] = flag.UintVar
	decs["UnquoteUsage"] = flag.UnquoteUsage
	decs["Usage"] = &flag.Usage
	decs["Value"] = reflect.TypeOf((*flag.Value)(nil)).Elem()
	decs["Var"] = flag.Var
	decs["Visit"] = flag.Visit
	decs["VisitAll"] = flag.VisitAll
	packages["flag"] = native.Package{
		Name:         "flag",
		Declarations: decs,
	}
	// "fmt"
	decs = make(native.Declarations, 25)
	decs["Errorf"] = fmt.Errorf
	decs["Formatter"] = reflect.TypeOf((*fmt.Formatter)(nil)).Elem()
	decs["Fprint"] = fmt.Fprint
	decs["Fprintf"] = fmt.Fprintf
	decs["Fprintln"] = fmt.Fprintln
	decs["Fscan"] = fmt.Fscan
	decs["Fscanf"] = fmt.Fscanf
	decs["Fscanln"] = fmt.Fscanln
	decs["GoStringer"] = reflect.TypeOf((*fmt.GoStringer)(nil)).Elem()
	decs["Print"] = fmt.Print
	decs["Printf"] = fmt.Printf
	decs["Println"] = fmt.Println
	decs["Scan"] = fmt.Scan
	decs["ScanState"] = reflect.TypeOf((*fmt.ScanState)(nil)).Elem()
	decs["Scanf"] = fmt.Scanf
	decs["Scanln"] = fmt.Scanln
	decs["Scanner"] = reflect.TypeOf((*fmt.Scanner)(nil)).Elem()
	decs["Sprint"] = fmt.Sprint
	decs["Sprintf"] = fmt.Sprintf
	decs["Sprintln"] = fmt.Sprintln
	decs["Sscan"] = fmt.Sscan
	decs["Sscanf"] = fmt.Sscanf
	decs["Sscanln"] = fmt.Sscanln
	decs["State"] = reflect.TypeOf((*fmt.State)(nil)).Elem()
	decs["Stringer"] = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	packages["fmt"] = native.Package{
		Name:         "fmt",
		Declarations: decs,
	}
	// "github.com/crewjam/rfc5424"
	decs = make(native.Declarations, 34)
	decs["Alert"] = rfc5424.Alert
	decs["Auth"] = rfc5424.Auth
	decs["Authpriv"] = rfc5424.Authpriv
	decs["Crit"] = rfc5424.Crit
	decs["Cron"] = rfc5424.Cron
	decs["Daemon"] = rfc5424.Daemon
	decs["Debug"] = rfc5424.Debug
	decs["Emergency"] = rfc5424.Emergency
	decs["ErrBadFormat"] = reflect.TypeOf((*rfc5424.ErrBadFormat)(nil)).Elem()
	decs["ErrInvalidValue"] = reflect.TypeOf((*rfc5424.ErrInvalidValue)(nil)).Elem()
	decs["Error"] = rfc5424.Error
	decs["Ftp"] = rfc5424.Ftp
	decs["Info"] = rfc5424.Info
	decs["Kern"] = rfc5424.Kern
	decs["Local0"] = rfc5424.Local0
	decs["Local1"] = rfc5424.Local1
	decs["Local2"] = rfc5424.Local2
	decs["Local3"] = rfc5424.Local3
	decs["Local4"] = rfc5424.Local4
	decs["Local5"] = rfc5424.Local5
	decs["Local6"] = rfc5424.Local6
	decs["Local7"] = rfc5424.Local7
	decs["Lpr"] = rfc5424.Lpr
	decs["Mail"] = rfc5424.Mail
	decs["Message"] = reflect.TypeOf((*rfc5424.Message)(nil)).Elem()
	decs["News"] = rfc5424.News
	decs["Notice"] = rfc5424.Notice
	decs["Priority"] = reflect.TypeOf((*rfc5424.Priority)(nil)).Elem()
	decs["SDParam"] = reflect.TypeOf((*rfc5424.SDParam)(nil)).Elem()
	decs["StructuredData"] = reflect.TypeOf((*rfc5424.StructuredData)(nil)).Elem()
	decs["Syslog"] = rfc5424.Syslog
	decs["User"] = rfc5424.User
	decs["Uucp"] = rfc5424.Uucp
	decs["Warning"] = rfc5424.Warning
	packages["github.com/crewjam/rfc5424"] = native.Package{
		Name:         "rfc5424",
		Declarations: decs,
	}
	// "github.com/dchest/safefile"
	decs = make(native.Declarations, 4)
	decs["Create"] = safefile.Create
	decs["ErrAlreadyCommitted"] = &safefile.ErrAlreadyCommitted
	decs["File"] = reflect.TypeOf((*safefile.File)(nil)).Elem()
	decs["WriteFile"] = safefile.WriteFile
	packages["github.com/dchest/safefile"] = native.Package{
		Name:         "safefile",
		Declarations: decs,
	}
	// "github.com/gobwas/glob"
	decs = make(native.Declarations, 4)
	decs["Compile"] = glob.Compile
	decs["Glob"] = reflect.TypeOf((*glob.Glob)(nil)).Elem()
	decs["MustCompile"] = glob.MustCompile
	decs["QuoteMeta"] = glob.QuoteMeta
	packages["github.com/gobwas/glob"] = native.Package{
		Name:         "glob",
		Declarations: decs,
	}
	// "github.com/gofrs/flock"
	decs = make(native.Declarations, 3)
	decs["Flock"] = reflect.TypeOf((*flock.Flock)(nil)).Elem()
	decs["New"] = flock.New
	decs["NewFlock"] = flock.NewFlock
	packages["github.com/gofrs/flock"] = native.Package{
		Name:         "flock",
		Declarations: decs,
	}
	// "github.com/google/gopacket"
	decs = make(native.Declarations, 83)
	decs["ApplicationLayer"] = reflect.TypeOf((*gopacket.ApplicationLayer)(nil)).Elem()
	decs["CaptureInfo"] = reflect.TypeOf((*gopacket.CaptureInfo)(nil)).Elem()
	decs["ConcatFinitePacketDataSources"] = gopacket.ConcatFinitePacketDataSources
	decs["DecodeFailure"] = reflect.TypeOf((*gopacket.DecodeFailure)(nil)).Elem()
	decs["DecodeFeedback"] = reflect.TypeOf((*gopacket.DecodeFeedback)(nil)).Elem()
	decs["DecodeFragment"] = &gopacket.DecodeFragment
	decs["DecodeFunc"] = reflect.TypeOf((*gopacket.DecodeFunc)(nil)).Elem()
	decs["DecodeOptions"] = reflect.TypeOf((*gopacket.DecodeOptions)(nil)).Elem()
	decs["DecodePayload"] = &gopacket.DecodePayload
	decs["DecodeStreamsAsDatagrams"] = &gopacket.DecodeStreamsAsDatagrams
	decs["DecodeUnknown"] = &gopacket.DecodeUnknown
	decs["Decoder"] = reflect.TypeOf((*gopacket.Decoder)(nil)).Elem()
	decs["DecodersByLayerName"] = &gopacket.DecodersByLayerName
	decs["DecodingLayer"] = reflect.TypeOf((*gopacket.DecodingLayer)(nil)).Elem()
	decs["DecodingLayerParser"] = reflect.TypeOf((*gopacket.DecodingLayerParser)(nil)).Elem()
	decs["DecodingLayerParserOptions"] = reflect.TypeOf((*gopacket.DecodingLayerParserOptions)(nil)).Elem()
	decs["Default"] = &gopacket.Default
	decs["Dumper"] = reflect.TypeOf((*gopacket.Dumper)(nil)).Elem()
	decs["Endpoint"] = reflect.TypeOf((*gopacket.Endpoint)(nil)).Elem()
	decs["EndpointInvalid"] = &gopacket.EndpointInvalid
	decs["EndpointType"] = reflect.TypeOf((*gopacket.EndpointType)(nil)).Elem()
	decs["EndpointTypeMetadata"] = reflect.TypeOf((*gopacket.EndpointTypeMetadata)(nil)).Elem()
	decs["ErrorLayer"] = reflect.TypeOf((*gopacket.ErrorLayer)(nil)).Elem()
	decs["Flow"] = reflect.TypeOf((*gopacket.Flow)(nil)).Elem()
	decs["FlowFromEndpoints"] = gopacket.FlowFromEndpoints
	decs["Fragment"] = reflect.TypeOf((*gopacket.Fragment)(nil)).Elem()
	decs["InvalidEndpoint"] = &gopacket.InvalidEndpoint
	decs["InvalidFlow"] = &gopacket.InvalidFlow
	decs["Layer"] = reflect.TypeOf((*gopacket.Layer)(nil)).Elem()
	decs["LayerClass"] = reflect.TypeOf((*gopacket.LayerClass)(nil)).Elem()
	decs["LayerClassMap"] = reflect.TypeOf((*gopacket.LayerClassMap)(nil)).Elem()
	decs["LayerClassSlice"] = reflect.TypeOf((*gopacket.LayerClassSlice)(nil)).Elem()
	decs["LayerDump"] = gopacket.LayerDump
	decs["LayerGoString"] = gopacket.LayerGoString
	decs["LayerString"] = gopacket.LayerString
	decs["LayerType"] = reflect.TypeOf((*gopacket.LayerType)(nil)).Elem()
	decs["LayerTypeDecodeFailure"] = &gopacket.LayerTypeDecodeFailure
	decs["LayerTypeFragment"] = &gopacket.LayerTypeFragment
	decs["LayerTypeMetadata"] = reflect.TypeOf((*gopacket.LayerTypeMetadata)(nil)).Elem()
	decs["LayerTypePayload"] = &gopacket.LayerTypePayload
	decs["LayerTypeZero"] = &gopacket.LayerTypeZero
	decs["Lazy"] = &gopacket.Lazy
	decs["LinkLayer"] = reflect.TypeOf((*gopacket.LinkLayer)(nil)).Elem()
	decs["LongBytesGoString"] = gopacket.LongBytesGoString
	decs["MaxEndpointSize"] = native.UntypedNumericConst("16")
	decs["NetworkLayer"] = reflect.TypeOf((*gopacket.NetworkLayer)(nil)).Elem()
	decs["NewDecodingLayerParser"] = gopacket.NewDecodingLayerParser
	decs["NewEndpoint"] = gopacket.NewEndpoint
	decs["NewFlow"] = gopacket.NewFlow
	decs["NewLayerClass"] = gopacket.NewLayerClass
	decs["NewLayerClassMap"] = gopacket.NewLayerClassMap
	decs["NewLayerClassSlice"] = gopacket.NewLayerClassSlice
	decs["NewPacket"] = gopacket.NewPacket
	decs["NewPacketSource"] = gopacket.NewPacketSource
	decs["NewSerializeBuffer"] = gopacket.NewSerializeBuffer
	decs["NewSerializeBufferExpectedSize"] = gopacket.NewSerializeBufferExpectedSize
	decs["NilDecodeFeedback"] = &gopacket.NilDecodeFeedback
	decs["NoCopy"] = &gopacket.NoCopy
	decs["OverrideLayerType"] = gopacket.OverrideLayerType
	decs["Packet"] = reflect.TypeOf((*gopacket.Packet)(nil)).Elem()
	decs["PacketBuilder"] = reflect.TypeOf((*gopacket.PacketBuilder)(nil)).Elem()
	decs["PacketDataSource"] = reflect.TypeOf((*gopacket.PacketDataSource)(nil)).Elem()
	decs["PacketMetadata"] = reflect.TypeOf((*gopacket.PacketMetadata)(nil)).Elem()
	decs["PacketSource"] = reflect.TypeOf((*gopacket.PacketSource)(nil)).Elem()
	decs["PacketSourceResolution"] = reflect.TypeOf((*gopacket.PacketSourceResolution)(nil)).Elem()
	decs["Payload"] = reflect.TypeOf((*gopacket.Payload)(nil)).Elem()
	decs["RegisterEndpointType"] = gopacket.RegisterEndpointType
	decs["RegisterLayerType"] = gopacket.RegisterLayerType
	decs["SerializableLayer"] = reflect.TypeOf((*gopacket.SerializableLayer)(nil)).Elem()
	decs["SerializeBuffer"] = reflect.TypeOf((*gopacket.SerializeBuffer)(nil)).Elem()
	decs["SerializeLayers"] = gopacket.SerializeLayers
	decs["SerializeOptions"] = reflect.TypeOf((*gopacket.SerializeOptions)(nil)).Elem()
	decs["SerializePacket"] = gopacket.SerializePacket
	decs["TimestampResolution"] = reflect.TypeOf((*gopacket.TimestampResolution)(nil)).Elem()
	decs["TimestampResolutionCaptureInfo"] = &gopacket.TimestampResolutionCaptureInfo
	decs["TimestampResolutionInvalid"] = &gopacket.TimestampResolutionInvalid
	decs["TimestampResolutionMicrosecond"] = &gopacket.TimestampResolutionMicrosecond
	decs["TimestampResolutionMillisecond"] = &gopacket.TimestampResolutionMillisecond
	decs["TimestampResolutionNTP"] = &gopacket.TimestampResolutionNTP
	decs["TimestampResolutionNanosecond"] = &gopacket.TimestampResolutionNanosecond
	decs["TransportLayer"] = reflect.TypeOf((*gopacket.TransportLayer)(nil)).Elem()
	decs["UnsupportedLayerType"] = reflect.TypeOf((*gopacket.UnsupportedLayerType)(nil)).Elem()
	decs["ZeroCopyPacketDataSource"] = reflect.TypeOf((*gopacket.ZeroCopyPacketDataSource)(nil)).Elem()
	packages["github.com/google/gopacket"] = native.Package{
		Name:         "gopacket",
		Declarations: decs,
	}
	// "github.com/google/renameio"
	decs = make(native.Declarations, 5)
	decs["PendingFile"] = reflect.TypeOf((*renameio.PendingFile)(nil)).Elem()
	decs["Symlink"] = renameio.Symlink
	decs["TempDir"] = renameio.TempDir
	decs["TempFile"] = renameio.TempFile
	decs["WriteFile"] = renameio.WriteFile
	packages["github.com/google/renameio"] = native.Package{
		Name:         "renameio",
		Declarations: decs,
	}
	// "github.com/google/uuid"
	decs = make(native.Declarations, 40)
	decs["ClockSequence"] = uuid.ClockSequence
	decs["Domain"] = reflect.TypeOf((*uuid.Domain)(nil)).Elem()
	decs["FromBytes"] = uuid.FromBytes
	decs["Future"] = uuid.Future
	decs["GetTime"] = uuid.GetTime
	decs["Group"] = uuid.Group
	decs["Invalid"] = uuid.Invalid
	decs["Microsoft"] = uuid.Microsoft
	decs["Must"] = uuid.Must
	decs["MustParse"] = uuid.MustParse
	decs["NameSpaceDNS"] = &uuid.NameSpaceDNS
	decs["NameSpaceOID"] = &uuid.NameSpaceOID
	decs["NameSpaceURL"] = &uuid.NameSpaceURL
	decs["NameSpaceX500"] = &uuid.NameSpaceX500
	decs["New"] = uuid.New
	decs["NewDCEGroup"] = uuid.NewDCEGroup
	decs["NewDCEPerson"] = uuid.NewDCEPerson
	decs["NewDCESecurity"] = uuid.NewDCESecurity
	decs["NewHash"] = uuid.NewHash
	decs["NewMD5"] = uuid.NewMD5
	decs["NewRandom"] = uuid.NewRandom
	decs["NewSHA1"] = uuid.NewSHA1
	decs["NewUUID"] = uuid.NewUUID
	decs["Nil"] = &uuid.Nil
	decs["NodeID"] = uuid.NodeID
	decs["NodeInterface"] = uuid.NodeInterface
	decs["Org"] = uuid.Org
	decs["Parse"] = uuid.Parse
	decs["ParseBytes"] = uuid.ParseBytes
	decs["Person"] = uuid.Person
	decs["RFC4122"] = uuid.RFC4122
	decs["Reserved"] = uuid.Reserved
	decs["SetClockSequence"] = uuid.SetClockSequence
	decs["SetNodeID"] = uuid.SetNodeID
	decs["SetNodeInterface"] = uuid.SetNodeInterface
	decs["SetRand"] = uuid.SetRand
	decs["Time"] = reflect.TypeOf((*uuid.Time)(nil)).Elem()
	decs["UUID"] = reflect.TypeOf((*uuid.UUID)(nil)).Elem()
	decs["Variant"] = reflect.TypeOf((*uuid.Variant)(nil)).Elem()
	decs["Version"] = reflect.TypeOf((*uuid.Version)(nil)).Elem()
	packages["github.com/google/uuid"] = native.Package{
		Name:         "uuid",
		Declarations: decs,
	}
	// "github.com/gravwell/gravwell/v3/ingest"
	decs = make(native.Declarations, 152)
	decs["ABSOLUTE_MAX_UNCONFIRMED_WRITES"] = ingest.ABSOLUTE_MAX_UNCONFIRMED_WRITES
	decs["ACK_SIZE"] = ingest.ACK_SIZE
	decs["ACK_WRITER_BUFFER_SIZE"] = ingest.ACK_WRITER_BUFFER_SIZE
	decs["ACK_WRITER_CAN_WRITE"] = ingest.ACK_WRITER_CAN_WRITE
	decs["API_VERSION_MAJOR"] = ingest.API_VERSION_MAJOR
	decs["API_VERSION_MINOR"] = ingest.API_VERSION_MINOR
	decs["API_VER_MAGIC"] = ingest.API_VER_MAGIC
	decs["AuthHash"] = reflect.TypeOf((*ingest.AuthHash)(nil)).Elem()
	decs["BUFFERED_ACK_READER_SIZE"] = ingest.BUFFERED_ACK_READER_SIZE
	decs["CHANNEL_BUFFER"] = ingest.CHANNEL_BUFFER
	decs["CLOSING_SERVICE_ACK_TIMEOUT"] = ingest.CLOSING_SERVICE_ACK_TIMEOUT
	decs["CONFIRM_API_VER_MAGIC"] = ingest.CONFIRM_API_VER_MAGIC
	decs["CONFIRM_ENTRY_MAGIC"] = ingest.CONFIRM_ENTRY_MAGIC
	decs["CONFIRM_ID_MAGIC"] = ingest.CONFIRM_ID_MAGIC
	decs["CONFIRM_INGESTER_STATE_MAGIC"] = ingest.CONFIRM_INGESTER_STATE_MAGIC
	decs["CONFIRM_INGEST_OK_MAGIC"] = ingest.CONFIRM_INGEST_OK_MAGIC
	decs["CONFIRM_TAG_MAGIC"] = ingest.CONFIRM_TAG_MAGIC
	decs["CacheModeAlways"] = native.UntypedStringConst("always")
	decs["CacheModeFail"] = native.UntypedStringConst("fail")
	decs["Challenge"] = reflect.TypeOf((*ingest.Challenge)(nil)).Elem()
	decs["ChallengeResponse"] = reflect.TypeOf((*ingest.ChallengeResponse)(nil)).Elem()
	decs["CheckTag"] = ingest.CheckTag
	decs["CompressNone"] = ingest.CompressNone
	decs["CompressSnappy"] = ingest.CompressSnappy
	decs["CompressionType"] = reflect.TypeOf((*ingest.CompressionType)(nil)).Elem()
	decs["ConnectionType"] = ingest.ConnectionType
	decs["DEFAULT_CLEAR_PORT"] = ingest.DEFAULT_CLEAR_PORT
	decs["DEFAULT_PIPE_PATH"] = ingest.DEFAULT_PIPE_PATH
	decs["DEFAULT_TLS_PORT"] = ingest.DEFAULT_TLS_PORT
	decs["DIAL_TIMEOUT"] = ingest.DIAL_TIMEOUT
	decs["ERROR_TAG_MAGIC"] = ingest.ERROR_TAG_MAGIC
	decs["EntryReader"] = reflect.TypeOf((*ingest.EntryReader)(nil)).Elem()
	decs["EntryReaderWriterConfig"] = reflect.TypeOf((*ingest.EntryReaderWriterConfig)(nil)).Elem()
	decs["EntryWriter"] = reflect.TypeOf((*ingest.EntryWriter)(nil)).Elem()
	decs["ErrAllConnsDown"] = &ingest.ErrAllConnsDown
	decs["ErrConnectionTimeout"] = &ingest.ErrConnectionTimeout
	decs["ErrEmergencyListOverflow"] = &ingest.ErrEmergencyListOverflow
	decs["ErrEmptyAuth"] = &ingest.ErrEmptyAuth
	decs["ErrEmptyTag"] = &ingest.ErrEmptyTag
	decs["ErrFailedAuth"] = &ingest.ErrFailedAuth
	decs["ErrFailedAuthHashGen"] = &ingest.ErrFailedAuthHashGen
	decs["ErrFailedParseLocalIP"] = &ingest.ErrFailedParseLocalIP
	decs["ErrFailedTagNegotiation"] = &ingest.ErrFailedTagNegotiation
	decs["ErrForbiddenTag"] = &ingest.ErrForbiddenTag
	decs["ErrInvalidBuffer"] = &ingest.ErrInvalidBuffer
	decs["ErrInvalidCerts"] = &ingest.ErrInvalidCerts
	decs["ErrInvalidConfigBlock"] = &ingest.ErrInvalidConfigBlock
	decs["ErrInvalidConnectionType"] = &ingest.ErrInvalidConnectionType
	decs["ErrInvalidDest"] = &ingest.ErrInvalidDest
	decs["ErrInvalidEntry"] = &ingest.ErrInvalidEntry
	decs["ErrInvalidIngestStateHeader"] = &ingest.ErrInvalidIngestStateHeader
	decs["ErrInvalidRemoteKeySize"] = &ingest.ErrInvalidRemoteKeySize
	decs["ErrInvalidSecret"] = &ingest.ErrInvalidSecret
	decs["ErrInvalidStateResponseLen"] = &ingest.ErrInvalidStateResponseLen
	decs["ErrInvalidTagRequestLen"] = &ingest.ErrInvalidTagRequestLen
	decs["ErrInvalidTagResponseLen"] = &ingest.ErrInvalidTagResponseLen
	decs["ErrMalformedDestination"] = &ingest.ErrMalformedDestination
	decs["ErrNoTargets"] = &ingest.ErrNoTargets
	decs["ErrNotReady"] = &ingest.ErrNotReady
	decs["ErrNotRunning"] = &ingest.ErrNotRunning
	decs["ErrOversizedEntry"] = &ingest.ErrOversizedEntry
	decs["ErrOversizedTag"] = &ingest.ErrOversizedTag
	decs["ErrSyncTimeout"] = &ingest.ErrSyncTimeout
	decs["ErrTagMapInvalid"] = &ingest.ErrTagMapInvalid
	decs["ErrTagNotFound"] = &ingest.ErrTagNotFound
	decs["ErrTimeout"] = &ingest.ErrTimeout
	decs["ErrWriteTimeout"] = &ingest.ErrWriteTimeout
	decs["FORBIDDEN_TAG_SET"] = ingest.FORBIDDEN_TAG_SET
	decs["FORCE_ACK_MAGIC"] = ingest.FORCE_ACK_MAGIC
	decs["G"] = native.UntypedNumericConst("1000000000.0")
	decs["GB"] = ingest.GB
	decs["GenAuthHash"] = ingest.GenAuthHash
	decs["GenerateResponse"] = ingest.GenerateResponse
	decs["HASH_ITERATIONS"] = ingest.HASH_ITERATIONS
	decs["HumanCount"] = ingest.HumanCount
	decs["HumanEntryRate"] = ingest.HumanEntryRate
	decs["HumanLineRate"] = ingest.HumanLineRate
	decs["HumanRate"] = ingest.HumanRate
	decs["HumanSize"] = ingest.HumanSize
	decs["ID_MAGIC"] = ingest.ID_MAGIC
	decs["INGESTER_STATE_MAGIC"] = ingest.INGESTER_STATE_MAGIC
	decs["INGEST_OK_MAGIC"] = ingest.INGEST_OK_MAGIC
	decs["INVALID_MAGIC"] = ingest.INVALID_MAGIC
	decs["IngestCommand"] = reflect.TypeOf((*ingest.IngestCommand)(nil)).Elem()
	decs["IngestConnection"] = reflect.TypeOf((*ingest.IngestConnection)(nil)).Elem()
	decs["IngestLogger"] = reflect.TypeOf((*ingest.IngestLogger)(nil)).Elem()
	decs["IngestMuxer"] = reflect.TypeOf((*ingest.IngestMuxer)(nil)).Elem()
	decs["IngesterState"] = reflect.TypeOf((*ingest.IngesterState)(nil)).Elem()
	decs["IngesterStateCallback"] = reflect.TypeOf((*ingest.IngesterStateCallback)(nil)).Elem()
	decs["InitializeConnection"] = ingest.InitializeConnection
	decs["K"] = native.UntypedNumericConst("1000.0")
	decs["KB"] = ingest.KB
	decs["Logger"] = reflect.TypeOf((*ingest.Logger)(nil)).Elem()
	decs["M"] = native.UntypedNumericConst("1000000.0")
	decs["MAX_ENTRY_SIZE"] = ingest.MAX_ENTRY_SIZE
	decs["MAX_TAG_LENGTH"] = ingest.MAX_TAG_LENGTH
	decs["MAX_UNCONFIRMED_COUNT"] = ingest.MAX_UNCONFIRMED_COUNT
	decs["MAX_WRITE_ERROR"] = ingest.MAX_WRITE_ERROR
	decs["MB"] = ingest.MB
	decs["MINIMUM_DYN_CONFIG_VERSION"] = ingest.MINIMUM_DYN_CONFIG_VERSION
	decs["MINIMUM_ID_VERSION"] = ingest.MINIMUM_ID_VERSION
	decs["MINIMUM_INGEST_OK_VERSION"] = ingest.MINIMUM_INGEST_OK_VERSION
	decs["MINIMUM_INGEST_STATE_VERSION"] = ingest.MINIMUM_INGEST_STATE_VERSION
	decs["MINIMUM_TAG_RENEGOTIATE_VERSION"] = ingest.MINIMUM_TAG_RENEGOTIATE_VERSION
	decs["MIN_REMOTE_KEYSIZE"] = ingest.MIN_REMOTE_KEYSIZE
	decs["MuxerConfig"] = reflect.TypeOf((*ingest.MuxerConfig)(nil)).Elem()
	decs["NEW_ENTRY_MAGIC"] = ingest.NEW_ENTRY_MAGIC
	decs["NewChallenge"] = ingest.NewChallenge
	decs["NewEntryReader"] = ingest.NewEntryReader
	decs["NewEntryReaderEx"] = ingest.NewEntryReaderEx
	decs["NewEntryWriter"] = ingest.NewEntryWriter
	decs["NewEntryWriterEx"] = ingest.NewEntryWriterEx
	decs["NewIngestMuxer"] = ingest.NewIngestMuxer
	decs["NewIngestMuxerExt"] = ingest.NewIngestMuxerExt
	decs["NewMuxer"] = ingest.NewMuxer
	decs["NewPipeConnection"] = ingest.NewPipeConnection
	decs["NewTCPConnection"] = ingest.NewTCPConnection
	decs["NewTLSConnection"] = ingest.NewTLSConnection
	decs["NewUniformIngestMuxer"] = ingest.NewUniformIngestMuxer
	decs["NewUniformIngestMuxerExt"] = ingest.NewUniformIngestMuxerExt
	decs["NewUniformMuxer"] = ingest.NewUniformMuxer
	decs["NoLogger"] = ingest.NoLogger
	decs["NsPerSec"] = ingest.NsPerSec
	decs["P"] = native.UntypedNumericConst("1000000000000.0")
	decs["PB"] = ingest.PB
	decs["PING_MAGIC"] = ingest.PING_MAGIC
	decs["PONG_MAGIC"] = ingest.PONG_MAGIC
	decs["ParseCompression"] = ingest.ParseCompression
	decs["PrintVersion"] = ingest.PrintVersion
	decs["READ_BUFFER_SIZE"] = ingest.READ_BUFFER_SIZE
	decs["READ_ENTRY_HEADER_SIZE"] = ingest.READ_ENTRY_HEADER_SIZE
	decs["STATE_AUTHENTICATED"] = ingest.STATE_AUTHENTICATED
	decs["STATE_HOT"] = ingest.STATE_HOT
	decs["STATE_NOT_AUTHENTICATED"] = ingest.STATE_NOT_AUTHENTICATED
	decs["StateResponse"] = reflect.TypeOf((*ingest.StateResponse)(nil)).Elem()
	decs["StreamConfiguration"] = reflect.TypeOf((*ingest.StreamConfiguration)(nil)).Elem()
	decs["T"] = native.UntypedNumericConst("1000000000000.0")
	decs["TAG_MAGIC"] = ingest.TAG_MAGIC
	decs["TB"] = ingest.TB
	decs["THROTTLE_MAGIC"] = ingest.THROTTLE_MAGIC
	decs["TLSCerts"] = reflect.TypeOf((*ingest.TLSCerts)(nil)).Elem()
	decs["TagManager"] = reflect.TypeOf((*ingest.TagManager)(nil)).Elem()
	decs["TagRequest"] = reflect.TypeOf((*ingest.TagRequest)(nil)).Elem()
	decs["TagResponse"] = reflect.TypeOf((*ingest.TagResponse)(nil)).Elem()
	decs["Target"] = reflect.TypeOf((*ingest.Target)(nil)).Elem()
	decs["TargetError"] = reflect.TypeOf((*ingest.TargetError)(nil)).Elem()
	decs["UniformMuxerConfig"] = reflect.TypeOf((*ingest.UniformMuxerConfig)(nil)).Elem()
	decs["VERSION"] = ingest.VERSION
	decs["VerifyResponse"] = ingest.VerifyResponse
	decs["WRITE_BUFFER_SIZE"] = ingest.WRITE_BUFFER_SIZE
	decs["Y"] = native.UntypedNumericConst("1000000000000000.0")
	decs["YB"] = ingest.YB
	packages["github.com/gravwell/gravwell/v3/ingest"] = native.Package{
		Name:         "ingest",
		Declarations: decs,
	}
	// "github.com/gravwell/gravwell/v3/ingest/config"
	decs = make(native.Declarations, 39)
	decs["AppendDefaultPort"] = config.AppendDefaultPort
	decs["CACHE_DEPTH_DEFAULT"] = native.UntypedNumericConst("128")
	decs["CACHE_MODE_DEFAULT"] = native.UntypedStringConst("always")
	decs["CACHE_SIZE_DEFAULT"] = native.UntypedNumericConst("1000")
	decs["CustomTimeFormat"] = reflect.TypeOf((*config.CustomTimeFormat)(nil)).Elem()
	decs["DefaultCleartextPort"] = config.DefaultCleartextPort
	decs["DefaultTLSPort"] = config.DefaultTLSPort
	decs["ErrBadMap"] = &config.ErrBadMap
	decs["ErrBadValue"] = &config.ErrBadValue
	decs["ErrConfigFileTooLarge"] = &config.ErrConfigFileTooLarge
	decs["ErrConfigNotOpen"] = &config.ErrConfigNotOpen
	decs["ErrEmptyEnvFile"] = &config.ErrEmptyEnvFile
	decs["ErrFailedFileRead"] = &config.ErrFailedFileRead
	decs["ErrGlobalSectionNotFound"] = &config.ErrGlobalSectionNotFound
	decs["ErrInvalidArg"] = &config.ErrInvalidArg
	decs["ErrInvalidArgument"] = &config.ErrInvalidArgument
	decs["ErrInvalidConnectionTimeout"] = &config.ErrInvalidConnectionTimeout
	decs["ErrInvalidImportInterface"] = &config.ErrInvalidImportInterface
	decs["ErrInvalidImportParameter"] = &config.ErrInvalidImportParameter
	decs["ErrInvalidLineLocation"] = &config.ErrInvalidLineLocation
	decs["ErrInvalidLogLevel"] = &config.ErrInvalidLogLevel
	decs["ErrInvalidMapKeyType"] = &config.ErrInvalidMapKeyType
	decs["ErrInvalidMapValueType"] = &config.ErrInvalidMapValueType
	decs["ErrInvalidUpdateLineParameter"] = &config.ErrInvalidUpdateLineParameter
	decs["ErrMissingIngestSecret"] = &config.ErrMissingIngestSecret
	decs["ErrNoConnections"] = &config.ErrNoConnections
	decs["ErrNotFound"] = &config.ErrNotFound
	decs["IngestConfig"] = reflect.TypeOf((*config.IngestConfig)(nil)).Elem()
	decs["IngestStreamConfig"] = reflect.TypeOf((*config.IngestStreamConfig)(nil)).Elem()
	decs["LoadConfigBytes"] = config.LoadConfigBytes
	decs["LoadConfigFile"] = config.LoadConfigFile
	decs["LoadEnvVar"] = config.LoadEnvVar
	decs["ParseBool"] = config.ParseBool
	decs["ParseInt64"] = config.ParseInt64
	decs["ParseRate"] = config.ParseRate
	decs["ParseSource"] = config.ParseSource
	decs["ParseUint64"] = config.ParseUint64
	decs["TimeFormat"] = reflect.TypeOf((*config.TimeFormat)(nil)).Elem()
	decs["VariableConfig"] = reflect.TypeOf((*config.VariableConfig)(nil)).Elem()
	packages["github.com/gravwell/gravwell/v3/ingest/config"] = native.Package{
		Name:         "config",
		Declarations: decs,
	}
	// "github.com/gravwell/gravwell/v3/ingest/entry"
	decs = make(native.Declarations, 41)
	decs["DefaultTagId"] = entry.DefaultTagId
	decs["DefaultTagName"] = entry.DefaultTagName
	decs["ENTRY_HEADER_SIZE"] = entry.ENTRY_HEADER_SIZE
	decs["Entry"] = reflect.TypeOf((*entry.Entry)(nil)).Elem()
	decs["EntryBlock"] = reflect.TypeOf((*entry.EntryBlock)(nil)).Elem()
	decs["EntryBlockHeaderSize"] = native.UntypedNumericConst("16")
	decs["EntryKey"] = reflect.TypeOf((*entry.EntryKey)(nil)).Elem()
	decs["EntrySlice"] = reflect.TypeOf((*entry.EntrySlice)(nil)).Elem()
	decs["EntryTag"] = reflect.TypeOf((*entry.EntryTag)(nil)).Elem()
	decs["ErrBadKey"] = &entry.ErrBadKey
	decs["ErrBlockTooLarge"] = &entry.ErrBlockTooLarge
	decs["ErrFailedBodyRead"] = &entry.ErrFailedBodyRead
	decs["ErrFailedBodyWrite"] = &entry.ErrFailedBodyWrite
	decs["ErrFailedHeaderWrite"] = &entry.ErrFailedHeaderWrite
	decs["ErrInvalidBufferSize"] = &entry.ErrInvalidBufferSize
	decs["ErrInvalidDestBuff"] = &entry.ErrInvalidDestBuff
	decs["ErrInvalidEntryBlock"] = &entry.ErrInvalidEntryBlock
	decs["ErrInvalidHeader"] = &entry.ErrInvalidHeader
	decs["ErrInvalidKey"] = &entry.ErrInvalidKey
	decs["ErrInvalidSrcBuff"] = &entry.ErrInvalidSrcBuff
	decs["ErrKeyAlreadySet"] = &entry.ErrKeyAlreadySet
	decs["ErrNilEntry"] = &entry.ErrNilEntry
	decs["ErrPartialDecode"] = &entry.ErrPartialDecode
	decs["ErrSliceLenTooLarge"] = &entry.ErrSliceLenTooLarge
	decs["ErrSliceSizeTooLarge"] = &entry.ErrSliceSizeTooLarge
	decs["ErrTSDataSizeInvalid"] = &entry.ErrTSDataSizeInvalid
	decs["FromStandard"] = entry.FromStandard
	decs["GravwellTagId"] = entry.GravwellTagId
	decs["GravwellTagName"] = entry.GravwellTagName
	decs["IPV4_SRC_SIZE"] = entry.IPV4_SRC_SIZE
	decs["MaxDataSize"] = entry.MaxDataSize
	decs["MaxSliceCount"] = entry.MaxSliceCount
	decs["NewDeepCopyEntryBlock"] = entry.NewDeepCopyEntryBlock
	decs["NewEntryBlock"] = entry.NewEntryBlock
	decs["NewEntryBlockNP"] = entry.NewEntryBlockNP
	decs["Now"] = entry.Now
	decs["SRC_SIZE"] = entry.SRC_SIZE
	decs["Since"] = entry.Since
	decs["TS_SIZE"] = entry.TS_SIZE
	decs["Timestamp"] = reflect.TypeOf((*entry.Timestamp)(nil)).Elem()
	decs["UnixTime"] = entry.UnixTime
	packages["github.com/gravwell/gravwell/v3/ingest/entry"] = native.Package{
		Name:         "entry",
		Declarations: decs,
	}
	// "github.com/gravwell/ipfix"
	decs = make(native.Declarations, 59)
	decs["Boolean"] = ipfix.Boolean
	decs["DataRecord"] = reflect.TypeOf((*ipfix.DataRecord)(nil)).Elem()
	decs["DateTimeMicroseconds"] = ipfix.DateTimeMicroseconds
	decs["DateTimeMilliseconds"] = ipfix.DateTimeMilliseconds
	decs["DateTimeNanoseconds"] = ipfix.DateTimeNanoseconds
	decs["DateTimeSeconds"] = ipfix.DateTimeSeconds
	decs["DictionaryEntry"] = reflect.TypeOf((*ipfix.DictionaryEntry)(nil)).Elem()
	decs["ErrNilCallback"] = &ipfix.ErrNilCallback
	decs["ErrProtocol"] = &ipfix.ErrProtocol
	decs["ErrRead"] = &ipfix.ErrRead
	decs["ErrUnknownTemplate"] = &ipfix.ErrUnknownTemplate
	decs["ErrVersion"] = &ipfix.ErrVersion
	decs["FieldType"] = reflect.TypeOf((*ipfix.FieldType)(nil)).Elem()
	decs["FieldTypes"] = &ipfix.FieldTypes
	decs["Filter"] = reflect.TypeOf((*ipfix.Filter)(nil)).Elem()
	decs["Float32"] = ipfix.Float32
	decs["Float64"] = ipfix.Float64
	decs["HeaderFilter"] = reflect.TypeOf((*ipfix.HeaderFilter)(nil)).Elem()
	decs["IPfixIDTypeLookup"] = ipfix.IPfixIDTypeLookup
	decs["Int16"] = ipfix.Int16
	decs["Int32"] = ipfix.Int32
	decs["Int64"] = ipfix.Int64
	decs["Int8"] = ipfix.Int8
	decs["InterpretedField"] = reflect.TypeOf((*ipfix.InterpretedField)(nil)).Elem()
	decs["InterpretedTemplateFieldSpecifier"] = reflect.TypeOf((*ipfix.InterpretedTemplateFieldSpecifier)(nil)).Elem()
	decs["Interpreter"] = reflect.TypeOf((*ipfix.Interpreter)(nil)).Elem()
	decs["IpfixIDLookup"] = ipfix.IpfixIDLookup
	decs["IpfixNameLookup"] = ipfix.IpfixNameLookup
	decs["Ipv4Address"] = ipfix.Ipv4Address
	decs["Ipv6Address"] = ipfix.Ipv6Address
	decs["LookupAndIdentify"] = ipfix.LookupAndIdentify
	decs["MacAddress"] = ipfix.MacAddress
	decs["Message"] = reflect.TypeOf((*ipfix.Message)(nil)).Elem()
	decs["MessageHeader"] = reflect.TypeOf((*ipfix.MessageHeader)(nil)).Elem()
	decs["NetflowV9IDLookup"] = ipfix.NetflowV9IDLookup
	decs["NetflowV9IDTypeLookup"] = ipfix.NetflowV9IDTypeLookup
	decs["NetflowV9NameLookup"] = ipfix.NetflowV9NameLookup
	decs["NewInterpreter"] = ipfix.NewInterpreter
	decs["NewInterpreterVersion"] = ipfix.NewInterpreterVersion
	decs["NewSession"] = ipfix.NewSession
	decs["NewWalker"] = ipfix.NewWalker
	decs["OctetArray"] = ipfix.OctetArray
	decs["Option"] = reflect.TypeOf((*ipfix.Option)(nil)).Elem()
	decs["Read"] = ipfix.Read
	decs["Record"] = reflect.TypeOf((*ipfix.Record)(nil)).Elem()
	decs["RecordCallback"] = reflect.TypeOf((*ipfix.RecordCallback)(nil)).Elem()
	decs["Session"] = reflect.TypeOf((*ipfix.Session)(nil)).Elem()
	decs["String"] = ipfix.String
	decs["TemplateFieldSpecifier"] = reflect.TypeOf((*ipfix.TemplateFieldSpecifier)(nil)).Elem()
	decs["TemplateRecord"] = reflect.TypeOf((*ipfix.TemplateRecord)(nil)).Elem()
	decs["Uint16"] = ipfix.Uint16
	decs["Uint24"] = ipfix.Uint24
	decs["Uint32"] = ipfix.Uint32
	decs["Uint64"] = ipfix.Uint64
	decs["Uint8"] = ipfix.Uint8
	decs["Unknown"] = ipfix.Unknown
	decs["VarInt"] = ipfix.VarInt
	decs["Walker"] = reflect.TypeOf((*ipfix.Walker)(nil)).Elem()
	decs["WithIDAliasing"] = ipfix.WithIDAliasing
	packages["github.com/gravwell/ipfix"] = native.Package{
		Name:         "ipfix",
		Declarations: decs,
	}
	// "github.com/h2non/filetype"
	decs = make(native.Declarations, 37)
	decs["AddMatcher"] = filetype.AddMatcher
	decs["AddType"] = filetype.AddType
	decs["Archive"] = filetype.Archive
	decs["Audio"] = filetype.Audio
	decs["Document"] = filetype.Document
	decs["ErrEmptyBuffer"] = &filetype.ErrEmptyBuffer
	decs["ErrUnknownBuffer"] = &filetype.ErrUnknownBuffer
	decs["Font"] = filetype.Font
	decs["Get"] = filetype.Get
	decs["GetType"] = filetype.GetType
	decs["Image"] = filetype.Image
	decs["Is"] = filetype.Is
	decs["IsArchive"] = filetype.IsArchive
	decs["IsAudio"] = filetype.IsAudio
	decs["IsDocument"] = filetype.IsDocument
	decs["IsExtension"] = filetype.IsExtension
	decs["IsFont"] = filetype.IsFont
	decs["IsImage"] = filetype.IsImage
	decs["IsMIME"] = filetype.IsMIME
	decs["IsMIMESupported"] = filetype.IsMIMESupported
	decs["IsSupported"] = filetype.IsSupported
	decs["IsType"] = filetype.IsType
	decs["IsVideo"] = filetype.IsVideo
	decs["Match"] = filetype.Match
	decs["MatchFile"] = filetype.MatchFile
	decs["MatchMap"] = filetype.MatchMap
	decs["MatchReader"] = filetype.MatchReader
	decs["MatcherKeys"] = &filetype.MatcherKeys
	decs["Matchers"] = &filetype.Matchers
	decs["Matches"] = filetype.Matches
	decs["MatchesMap"] = filetype.MatchesMap
	decs["NewMatcher"] = &filetype.NewMatcher
	decs["NewType"] = &filetype.NewType
	decs["Types"] = &filetype.Types
	decs["Unknown"] = &filetype.Unknown
	decs["Version"] = native.UntypedStringConst("1.0.10")
	decs["Video"] = filetype.Video
	packages["github.com/h2non/filetype"] = native.Package{
		Name:         "filetype",
		Declarations: decs,
	}
	// "github.com/k-sone/ipmigo"
	decs = make(native.Declarations, 111)
	decs["ArgumentError"] = reflect.TypeOf((*ipmigo.ArgumentError)(nil)).Elem()
	decs["Arguments"] = reflect.TypeOf((*ipmigo.Arguments)(nil)).Elem()
	decs["Client"] = reflect.TypeOf((*ipmigo.Client)(nil)).Elem()
	decs["Command"] = reflect.TypeOf((*ipmigo.Command)(nil)).Elem()
	decs["CommandError"] = reflect.TypeOf((*ipmigo.CommandError)(nil)).Elem()
	decs["CompletionBMCInitialization"] = ipmigo.CompletionBMCInitialization
	decs["CompletionCantBeProvided"] = ipmigo.CompletionCantBeProvided
	decs["CompletionCantReturnDataBytes"] = ipmigo.CompletionCantReturnDataBytes
	decs["CompletionCode"] = reflect.TypeOf((*ipmigo.CompletionCode)(nil)).Elem()
	decs["CompletionDestinationUnavailable"] = ipmigo.CompletionDestinationUnavailable
	decs["CompletionDuplicatedRequest"] = ipmigo.CompletionDuplicatedRequest
	decs["CompletionFirmwareUpdateMode"] = ipmigo.CompletionFirmwareUpdateMode
	decs["CompletionIllegalCommandDisabled"] = ipmigo.CompletionIllegalCommandDisabled
	decs["CompletionIllegalSendorOrRecord"] = ipmigo.CompletionIllegalSendorOrRecord
	decs["CompletionInsufficientPrivilege"] = ipmigo.CompletionInsufficientPrivilege
	decs["CompletionInvalidCommand"] = ipmigo.CompletionInvalidCommand
	decs["CompletionInvalidCommandForLUN"] = ipmigo.CompletionInvalidCommandForLUN
	decs["CompletionInvalidDataField"] = ipmigo.CompletionInvalidDataField
	decs["CompletionNodeBusy"] = ipmigo.CompletionNodeBusy
	decs["CompletionNotSupportedPresentState"] = ipmigo.CompletionNotSupportedPresentState
	decs["CompletionOK"] = ipmigo.CompletionOK
	decs["CompletionOutOfSpace"] = ipmigo.CompletionOutOfSpace
	decs["CompletionParameterOutOfRange"] = ipmigo.CompletionParameterOutOfRange
	decs["CompletionRequestDataFieldExceedEd"] = ipmigo.CompletionRequestDataFieldExceedEd
	decs["CompletionRequestDataInvalidLength"] = ipmigo.CompletionRequestDataInvalidLength
	decs["CompletionRequestDataNotPresent"] = ipmigo.CompletionRequestDataNotPresent
	decs["CompletionRequestDataTruncated"] = ipmigo.CompletionRequestDataTruncated
	decs["CompletionReservationCancelled"] = ipmigo.CompletionReservationCancelled
	decs["CompletionSDRInUpdateMode"] = ipmigo.CompletionSDRInUpdateMode
	decs["CompletionTimeout"] = ipmigo.CompletionTimeout
	decs["CompletionUnspecifiedError"] = ipmigo.CompletionUnspecifiedError
	decs["ErrNotSupportedIPMI"] = &ipmigo.ErrNotSupportedIPMI
	decs["EventType"] = reflect.TypeOf((*ipmigo.EventType)(nil)).Elem()
	decs["GetChassisStatusCommand"] = reflect.TypeOf((*ipmigo.GetChassisStatusCommand)(nil)).Elem()
	decs["GetDeviceIDCommand"] = reflect.TypeOf((*ipmigo.GetDeviceIDCommand)(nil)).Elem()
	decs["GetPOHCounterCommand"] = reflect.TypeOf((*ipmigo.GetPOHCounterCommand)(nil)).Elem()
	decs["GetSDRCommand"] = reflect.TypeOf((*ipmigo.GetSDRCommand)(nil)).Elem()
	decs["GetSDRRepositoryInfoCommand"] = reflect.TypeOf((*ipmigo.GetSDRRepositoryInfoCommand)(nil)).Elem()
	decs["GetSELEntryCommand"] = reflect.TypeOf((*ipmigo.GetSELEntryCommand)(nil)).Elem()
	decs["GetSELInfoCommand"] = reflect.TypeOf((*ipmigo.GetSELInfoCommand)(nil)).Elem()
	decs["GetSensorReadingCommand"] = reflect.TypeOf((*ipmigo.GetSensorReadingCommand)(nil)).Elem()
	decs["GetSessionInfoCommand"] = reflect.TypeOf((*ipmigo.GetSessionInfoCommand)(nil)).Elem()
	decs["GetSystemRestartCauseCommand"] = reflect.TypeOf((*ipmigo.GetSystemRestartCauseCommand)(nil)).Elem()
	decs["MessageError"] = reflect.TypeOf((*ipmigo.MessageError)(nil)).Elem()
	decs["NetFn"] = reflect.TypeOf((*ipmigo.NetFn)(nil)).Elem()
	decs["NetFnAppReq"] = ipmigo.NetFnAppReq
	decs["NetFnAppRes"] = ipmigo.NetFnAppRes
	decs["NetFnBridgeReq"] = ipmigo.NetFnBridgeReq
	decs["NetFnBridgeRes"] = ipmigo.NetFnBridgeRes
	decs["NetFnChassisReq"] = ipmigo.NetFnChassisReq
	decs["NetFnChassisRes"] = ipmigo.NetFnChassisRes
	decs["NetFnFirmwareReq"] = ipmigo.NetFnFirmwareReq
	decs["NetFnFirmwareRes"] = ipmigo.NetFnFirmwareRes
	decs["NetFnRsLUN"] = reflect.TypeOf((*ipmigo.NetFnRsLUN)(nil)).Elem()
	decs["NetFnSensorReq"] = ipmigo.NetFnSensorReq
	decs["NetFnSensorRes"] = ipmigo.NetFnSensorRes
	decs["NetFnStorageReq"] = ipmigo.NetFnStorageReq
	decs["NetFnStorageRes"] = ipmigo.NetFnStorageRes
	decs["NetFnTransportReq"] = ipmigo.NetFnTransportReq
	decs["NetFnTransportRes"] = ipmigo.NetFnTransportRes
	decs["NewClient"] = ipmigo.NewClient
	decs["NewNetFnRsLUN"] = ipmigo.NewNetFnRsLUN
	decs["NewRawCommand"] = ipmigo.NewRawCommand
	decs["NewThresholdStatus"] = ipmigo.NewThresholdStatus
	decs["PrivilegeAdministrator"] = ipmigo.PrivilegeAdministrator
	decs["PrivilegeCallback"] = ipmigo.PrivilegeCallback
	decs["PrivilegeLevel"] = reflect.TypeOf((*ipmigo.PrivilegeLevel)(nil)).Elem()
	decs["PrivilegeOperator"] = ipmigo.PrivilegeOperator
	decs["PrivilegeUser"] = ipmigo.PrivilegeUser
	decs["RawCommand"] = reflect.TypeOf((*ipmigo.RawCommand)(nil)).Elem()
	decs["ReserveSDRRepositoryCommand"] = reflect.TypeOf((*ipmigo.ReserveSDRRepositoryCommand)(nil)).Elem()
	decs["ReserveSELCommand"] = reflect.TypeOf((*ipmigo.ReserveSELCommand)(nil)).Elem()
	decs["SDR"] = reflect.TypeOf((*ipmigo.SDR)(nil)).Elem()
	decs["SDRCommonSensor"] = reflect.TypeOf((*ipmigo.SDRCommonSensor)(nil)).Elem()
	decs["SDRCompactSensor"] = reflect.TypeOf((*ipmigo.SDRCompactSensor)(nil)).Elem()
	decs["SDRFRUDeviceLocator"] = reflect.TypeOf((*ipmigo.SDRFRUDeviceLocator)(nil)).Elem()
	decs["SDRFullSensor"] = reflect.TypeOf((*ipmigo.SDRFullSensor)(nil)).Elem()
	decs["SDRGetAllRecordsRepo"] = ipmigo.SDRGetAllRecordsRepo
	decs["SDRGetRecordsRepo"] = ipmigo.SDRGetRecordsRepo
	decs["SDRType"] = reflect.TypeOf((*ipmigo.SDRType)(nil)).Elem()
	decs["SDRTypeBMCMessageChannelInfo"] = ipmigo.SDRTypeBMCMessageChannelInfo
	decs["SDRTypeCompactSensor"] = ipmigo.SDRTypeCompactSensor
	decs["SDRTypeDeviceEntityAssociation"] = ipmigo.SDRTypeDeviceEntityAssociation
	decs["SDRTypeEntityAssociation"] = ipmigo.SDRTypeEntityAssociation
	decs["SDRTypeEventOnlySensor"] = ipmigo.SDRTypeEventOnlySensor
	decs["SDRTypeFRUDeviceLocator"] = ipmigo.SDRTypeFRUDeviceLocator
	decs["SDRTypeFullSensor"] = ipmigo.SDRTypeFullSensor
	decs["SDRTypeGenericDeviceLocator"] = ipmigo.SDRTypeGenericDeviceLocator
	decs["SDRTypeMCConfirmation"] = ipmigo.SDRTypeMCConfirmation
	decs["SDRTypeMCDeviceLocator"] = ipmigo.SDRTypeMCDeviceLocator
	decs["SDRTypeOEM"] = ipmigo.SDRTypeOEM
	decs["SELEventRecord"] = reflect.TypeOf((*ipmigo.SELEventRecord)(nil)).Elem()
	decs["SELGetEntries"] = ipmigo.SELGetEntries
	decs["SELNonTimestampedOEMRecord"] = reflect.TypeOf((*ipmigo.SELNonTimestampedOEMRecord)(nil)).Elem()
	decs["SELRecord"] = reflect.TypeOf((*ipmigo.SELRecord)(nil)).Elem()
	decs["SELTimestampedOEMRecord"] = reflect.TypeOf((*ipmigo.SELTimestampedOEMRecord)(nil)).Elem()
	decs["SELType"] = reflect.TypeOf((*ipmigo.SELType)(nil)).Elem()
	decs["SensorType"] = reflect.TypeOf((*ipmigo.SensorType)(nil)).Elem()
	decs["ThresholdStatus"] = reflect.TypeOf((*ipmigo.ThresholdStatus)(nil)).Elem()
	decs["ThresholdStatusLCR"] = ipmigo.ThresholdStatusLCR
	decs["ThresholdStatusLNC"] = ipmigo.ThresholdStatusLNC
	decs["ThresholdStatusLNR"] = ipmigo.ThresholdStatusLNR
	decs["ThresholdStatusOK"] = ipmigo.ThresholdStatusOK
	decs["ThresholdStatusUCR"] = ipmigo.ThresholdStatusUCR
	decs["ThresholdStatusUNC"] = ipmigo.ThresholdStatusUNC
	decs["ThresholdStatusUNR"] = ipmigo.ThresholdStatusUNR
	decs["Timestamp"] = reflect.TypeOf((*ipmigo.Timestamp)(nil)).Elem()
	decs["UnitType"] = reflect.TypeOf((*ipmigo.UnitType)(nil)).Elem()
	decs["V1_5"] = ipmigo.V1_5
	decs["V2_0"] = ipmigo.V2_0
	decs["Version"] = reflect.TypeOf((*ipmigo.Version)(nil)).Elem()
	packages["github.com/k-sone/ipmigo"] = native.Package{
		Name:         "ipmigo",
		Declarations: decs,
	}
	// "github.com/klauspost/compress"
	decs = make(native.Declarations, 2)
	decs["Estimate"] = compress.Estimate
	decs["ShannonEntropyBits"] = compress.ShannonEntropyBits
	packages["github.com/klauspost/compress"] = native.Package{
		Name:         "compress",
		Declarations: decs,
	}
	// "github.com/open-networks/go-msgraph"
	decs = make(native.Declarations, 46)
	decs["APIVersion"] = msgraph.APIVersion
	decs["Alert"] = reflect.TypeOf((*msgraph.Alert)(nil)).Elem()
	decs["AlertTrigger"] = reflect.TypeOf((*msgraph.AlertTrigger)(nil)).Elem()
	decs["Attendee"] = reflect.TypeOf((*msgraph.Attendee)(nil)).Elem()
	decs["Attendees"] = reflect.TypeOf((*msgraph.Attendees)(nil)).Elem()
	decs["AverageComparativeScore"] = reflect.TypeOf((*msgraph.AverageComparativeScore)(nil)).Elem()
	decs["BaseURL"] = msgraph.BaseURL
	decs["Calendar"] = reflect.TypeOf((*msgraph.Calendar)(nil)).Elem()
	decs["CalendarEvent"] = reflect.TypeOf((*msgraph.CalendarEvent)(nil)).Elem()
	decs["CalendarEvents"] = reflect.TypeOf((*msgraph.CalendarEvents)(nil)).Elem()
	decs["Calendars"] = reflect.TypeOf((*msgraph.Calendars)(nil)).Elem()
	decs["CertificationControl"] = reflect.TypeOf((*msgraph.CertificationControl)(nil)).Elem()
	decs["CloudAppSecurityState"] = reflect.TypeOf((*msgraph.CloudAppSecurityState)(nil)).Elem()
	decs["ComplianceInformation"] = reflect.TypeOf((*msgraph.ComplianceInformation)(nil)).Elem()
	decs["ControlScore"] = reflect.TypeOf((*msgraph.ControlScore)(nil)).Elem()
	decs["EmailAddress"] = reflect.TypeOf((*msgraph.EmailAddress)(nil)).Elem()
	decs["ErrFindCalendar"] = &msgraph.ErrFindCalendar
	decs["ErrFindGroup"] = &msgraph.ErrFindGroup
	decs["ErrFindUser"] = &msgraph.ErrFindUser
	decs["ErrNotGraphClientSourced"] = &msgraph.ErrNotGraphClientSourced
	decs["FileHash"] = reflect.TypeOf((*msgraph.FileHash)(nil)).Elem()
	decs["FileSecurityState"] = reflect.TypeOf((*msgraph.FileSecurityState)(nil)).Elem()
	decs["FullDayEventTimeZone"] = &msgraph.FullDayEventTimeZone
	decs["GraphClient"] = reflect.TypeOf((*msgraph.GraphClient)(nil)).Elem()
	decs["Group"] = reflect.TypeOf((*msgraph.Group)(nil)).Elem()
	decs["Groups"] = reflect.TypeOf((*msgraph.Groups)(nil)).Elem()
	decs["HostSecurityState"] = reflect.TypeOf((*msgraph.HostSecurityState)(nil)).Elem()
	decs["LoginBaseURL"] = msgraph.LoginBaseURL
	decs["MalwareState"] = reflect.TypeOf((*msgraph.MalwareState)(nil)).Elem()
	decs["MaxPageSize"] = msgraph.MaxPageSize
	decs["NetworkConnection"] = reflect.TypeOf((*msgraph.NetworkConnection)(nil)).Elem()
	decs["NewGraphClient"] = msgraph.NewGraphClient
	decs["Process"] = reflect.TypeOf((*msgraph.Process)(nil)).Elem()
	decs["RegistryKeyState"] = reflect.TypeOf((*msgraph.RegistryKeyState)(nil)).Elem()
	decs["ResponseStatus"] = reflect.TypeOf((*msgraph.ResponseStatus)(nil)).Elem()
	decs["SecureScore"] = reflect.TypeOf((*msgraph.SecureScore)(nil)).Elem()
	decs["SecureScoreControlProfile"] = reflect.TypeOf((*msgraph.SecureScoreControlProfile)(nil)).Elem()
	decs["SecureScoreControlStateUpdate"] = reflect.TypeOf((*msgraph.SecureScoreControlStateUpdate)(nil)).Elem()
	decs["SecurityResource"] = reflect.TypeOf((*msgraph.SecurityResource)(nil)).Elem()
	decs["SecurityVendorInformation"] = reflect.TypeOf((*msgraph.SecurityVendorInformation)(nil)).Elem()
	decs["Token"] = reflect.TypeOf((*msgraph.Token)(nil)).Elem()
	decs["User"] = reflect.TypeOf((*msgraph.User)(nil)).Elem()
	decs["UserSecurityState"] = reflect.TypeOf((*msgraph.UserSecurityState)(nil)).Elem()
	decs["Users"] = reflect.TypeOf((*msgraph.Users)(nil)).Elem()
	decs["VulnerabilityState"] = reflect.TypeOf((*msgraph.VulnerabilityState)(nil)).Elem()
	decs["WinIANA"] = &msgraph.WinIANA
	packages["github.com/open-networks/go-msgraph"] = native.Package{
		Name:         "msgraph",
		Declarations: decs,
	}
	// "github.com/tealeg/xlsx"
	decs = make(native.Declarations, 166)
	decs["Alignment"] = reflect.TypeOf((*xlsx.Alignment)(nil)).Elem()
	decs["AlreadyOnLastSheetError"] = &xlsx.AlreadyOnLastSheetError
	decs["AutoFilter"] = reflect.TypeOf((*xlsx.AutoFilter)(nil)).Elem()
	decs["Baskerville"] = native.UntypedStringConst("Baskerville Old Face")
	decs["Bodoni"] = native.UntypedStringConst("Bodoni MT")
	decs["Border"] = reflect.TypeOf((*xlsx.Border)(nil)).Elem()
	decs["BuiltStreamFileBuilderError"] = &xlsx.BuiltStreamFileBuilderError
	decs["Cell"] = reflect.TypeOf((*xlsx.Cell)(nil)).Elem()
	decs["CellInterface"] = reflect.TypeOf((*xlsx.CellInterface)(nil)).Elem()
	decs["CellMetadata"] = reflect.TypeOf((*xlsx.CellMetadata)(nil)).Elem()
	decs["CellType"] = reflect.TypeOf((*xlsx.CellType)(nil)).Elem()
	decs["CellTypeBool"] = xlsx.CellTypeBool
	decs["CellTypeDate"] = xlsx.CellTypeDate
	decs["CellTypeError"] = xlsx.CellTypeError
	decs["CellTypeInline"] = xlsx.CellTypeInline
	decs["CellTypeNumeric"] = xlsx.CellTypeNumeric
	decs["CellTypeString"] = xlsx.CellTypeString
	decs["CellTypeStringFormula"] = xlsx.CellTypeStringFormula
	decs["Col"] = reflect.TypeOf((*xlsx.Col)(nil)).Elem()
	decs["ColIndexToLetters"] = xlsx.ColIndexToLetters
	decs["ColLettersToIndex"] = xlsx.ColLettersToIndex
	decs["ColWidth"] = native.UntypedNumericConst("9.5")
	decs["Courier"] = native.UntypedStringConst("Courier")
	decs["DataValidationErrorStyle"] = reflect.TypeOf((*xlsx.DataValidationErrorStyle)(nil)).Elem()
	decs["DataValidationOperator"] = reflect.TypeOf((*xlsx.DataValidationOperator)(nil)).Elem()
	decs["DataValidationOperatorBetween"] = native.UntypedNumericConst("1")
	decs["DataValidationOperatorEqual"] = native.UntypedNumericConst("2")
	decs["DataValidationOperatorGreaterThan"] = native.UntypedNumericConst("3")
	decs["DataValidationOperatorGreaterThanOrEqual"] = native.UntypedNumericConst("4")
	decs["DataValidationOperatorLessThan"] = native.UntypedNumericConst("5")
	decs["DataValidationOperatorLessThanOrEqual"] = native.UntypedNumericConst("6")
	decs["DataValidationOperatorNotBetween"] = native.UntypedNumericConst("7")
	decs["DataValidationOperatorNotEqual"] = native.UntypedNumericConst("8")
	decs["DataValidationType"] = reflect.TypeOf((*xlsx.DataValidationType)(nil)).Elem()
	decs["DataValidationTypeCustom"] = native.UntypedNumericConst("2")
	decs["DataValidationTypeDate"] = native.UntypedNumericConst("3")
	decs["DataValidationTypeDecimal"] = native.UntypedNumericConst("4")
	decs["DataValidationTypeTextLeng"] = native.UntypedNumericConst("6")
	decs["DataValidationTypeTime"] = native.UntypedNumericConst("7")
	decs["DataValidationTypeWhole"] = native.UntypedNumericConst("8")
	decs["DateFormat_dd_mm_yy"] = native.UntypedNumericConst("14")
	decs["DateTimeFormat_d_m_yy_h_mm"] = native.UntypedNumericConst("22")
	decs["DateTimeOptions"] = reflect.TypeOf((*xlsx.DateTimeOptions)(nil)).Elem()
	decs["DecimalFormat"] = native.UntypedNumericConst("2")
	decs["DefaultAlignment"] = xlsx.DefaultAlignment
	decs["DefaultBorder"] = xlsx.DefaultBorder
	decs["DefaultDateCellMetadata"] = &xlsx.DefaultDateCellMetadata
	decs["DefaultDateFormat"] = &xlsx.DefaultDateFormat
	decs["DefaultDateOptions"] = &xlsx.DefaultDateOptions
	decs["DefaultDateTimeFormat"] = &xlsx.DefaultDateTimeFormat
	decs["DefaultDateTimeOptions"] = &xlsx.DefaultDateTimeOptions
	decs["DefaultDecimalCellMetadata"] = &xlsx.DefaultDecimalCellMetadata
	decs["DefaultFill"] = xlsx.DefaultFill
	decs["DefaultFont"] = xlsx.DefaultFont
	decs["DefaultIntegerCellMetadata"] = &xlsx.DefaultIntegerCellMetadata
	decs["DefaultNumericCellMetadata"] = &xlsx.DefaultNumericCellMetadata
	decs["DefaultStringCellMetadata"] = &xlsx.DefaultStringCellMetadata
	decs["Excel2006MaxRowCount"] = native.UntypedNumericConst("1048576")
	decs["Excel2006MaxRowIndex"] = native.UntypedNumericConst("1048575")
	decs["File"] = reflect.TypeOf((*xlsx.File)(nil)).Elem()
	decs["FileToSlice"] = xlsx.FileToSlice
	decs["FileToSliceUnmerged"] = xlsx.FileToSliceUnmerged
	decs["Fill"] = reflect.TypeOf((*xlsx.Fill)(nil)).Elem()
	decs["FillGreen"] = &xlsx.FillGreen
	decs["FillRed"] = &xlsx.FillRed
	decs["FillWhite"] = &xlsx.FillWhite
	decs["Font"] = reflect.TypeOf((*xlsx.Font)(nil)).Elem()
	decs["FontBold"] = &xlsx.FontBold
	decs["FontItalic"] = &xlsx.FontItalic
	decs["FontUnderlined"] = &xlsx.FontUnderlined
	decs["GeneralFormat"] = native.UntypedNumericConst("0")
	decs["GetCellIDStringFromCoords"] = xlsx.GetCellIDStringFromCoords
	decs["GetCellIDStringFromCoordsWithFixed"] = xlsx.GetCellIDStringFromCoordsWithFixed
	decs["GetCoordsFromCellIDString"] = xlsx.GetCoordsFromCellIDString
	decs["GillSans"] = native.UntypedStringConst("Gill Sans MT")
	decs["HSL"] = reflect.TypeOf((*xlsx.HSL)(nil)).Elem()
	decs["HSLModel"] = &xlsx.HSLModel
	decs["HSLToRGB"] = xlsx.HSLToRGB
	decs["Helvetica"] = native.UntypedStringConst("Helvetica")
	decs["IntegerFormat"] = native.UntypedNumericConst("1")
	decs["MJD_0"] = xlsx.MJD_0
	decs["MJD_JD2000"] = xlsx.MJD_JD2000
	decs["MakeCellMetadata"] = xlsx.MakeCellMetadata
	decs["MakeDateStyle"] = xlsx.MakeDateStyle
	decs["MakeDecimalStyle"] = xlsx.MakeDecimalStyle
	decs["MakeDefaultContentTypes"] = xlsx.MakeDefaultContentTypes
	decs["MakeIntegerStyle"] = xlsx.MakeIntegerStyle
	decs["MakeSharedStringRefTable"] = xlsx.MakeSharedStringRefTable
	decs["MakeStringStyle"] = xlsx.MakeStringStyle
	decs["MakeStyle"] = xlsx.MakeStyle
	decs["NewBorder"] = xlsx.NewBorder
	decs["NewCell"] = xlsx.NewCell
	decs["NewDateStreamCell"] = xlsx.NewDateStreamCell
	decs["NewFile"] = xlsx.NewFile
	decs["NewFill"] = xlsx.NewFill
	decs["NewFont"] = xlsx.NewFont
	decs["NewIntegerStreamCell"] = xlsx.NewIntegerStreamCell
	decs["NewSharedStringRefTable"] = xlsx.NewSharedStringRefTable
	decs["NewStreamCell"] = xlsx.NewStreamCell
	decs["NewStreamFileBuilder"] = xlsx.NewStreamFileBuilder
	decs["NewStreamFileBuilderForPath"] = xlsx.NewStreamFileBuilderForPath
	decs["NewStringStreamCell"] = xlsx.NewStringStreamCell
	decs["NewStyle"] = xlsx.NewStyle
	decs["NewStyledIntegerStreamCell"] = xlsx.NewStyledIntegerStreamCell
	decs["NewStyledStringStreamCell"] = xlsx.NewStyledStringStreamCell
	decs["NewXlsxCellDataValidation"] = xlsx.NewXlsxCellDataValidation
	decs["NoCurrentSheetError"] = &xlsx.NoCurrentSheetError
	decs["NoRowLimit"] = xlsx.NoRowLimit
	decs["OpenBinary"] = xlsx.OpenBinary
	decs["OpenBinaryWithRowLimit"] = xlsx.OpenBinaryWithRowLimit
	decs["OpenFile"] = xlsx.OpenFile
	decs["OpenFileWithRowLimit"] = xlsx.OpenFileWithRowLimit
	decs["OpenReaderAt"] = xlsx.OpenReaderAt
	decs["OpenReaderAtWithRowLimit"] = xlsx.OpenReaderAtWithRowLimit
	decs["Pane"] = reflect.TypeOf((*xlsx.Pane)(nil)).Elem()
	decs["RGBToHSL"] = xlsx.RGBToHSL
	decs["RGB_Black"] = native.UntypedStringConst("FFFFFFFF")
	decs["RGB_Dark_Green"] = native.UntypedStringConst("FF006100")
	decs["RGB_Dark_Red"] = native.UntypedStringConst("FF9C0006")
	decs["RGB_Light_Green"] = native.UntypedStringConst("FFC6EFCE")
	decs["RGB_Light_Red"] = native.UntypedStringConst("FFFFC7CE")
	decs["RGB_White"] = native.UntypedStringConst("00000000")
	decs["ReadZip"] = xlsx.ReadZip
	decs["ReadZipReader"] = xlsx.ReadZipReader
	decs["ReadZipReaderWithRowLimit"] = xlsx.ReadZipReaderWithRowLimit
	decs["ReadZipWithRowLimit"] = xlsx.ReadZipWithRowLimit
	decs["RefTable"] = reflect.TypeOf((*xlsx.RefTable)(nil)).Elem()
	decs["Row"] = reflect.TypeOf((*xlsx.Row)(nil)).Elem()
	decs["RowIndexToString"] = xlsx.RowIndexToString
	decs["SetDefaultFont"] = xlsx.SetDefaultFont
	decs["Sheet"] = reflect.TypeOf((*xlsx.Sheet)(nil)).Elem()
	decs["SheetFormat"] = reflect.TypeOf((*xlsx.SheetFormat)(nil)).Elem()
	decs["SheetView"] = reflect.TypeOf((*xlsx.SheetView)(nil)).Elem()
	decs["Solid_Cell_Fill"] = native.UntypedStringConst("solid")
	decs["StreamCell"] = reflect.TypeOf((*xlsx.StreamCell)(nil)).Elem()
	decs["StreamFile"] = reflect.TypeOf((*xlsx.StreamFile)(nil)).Elem()
	decs["StreamFileBuilder"] = reflect.TypeOf((*xlsx.StreamFileBuilder)(nil)).Elem()
	decs["StreamStyle"] = reflect.TypeOf((*xlsx.StreamStyle)(nil)).Elem()
	decs["StreamStyleBoldInteger"] = &xlsx.StreamStyleBoldInteger
	decs["StreamStyleBoldString"] = &xlsx.StreamStyleBoldString
	decs["StreamStyleDefaultDate"] = &xlsx.StreamStyleDefaultDate
	decs["StreamStyleDefaultDecimal"] = &xlsx.StreamStyleDefaultDecimal
	decs["StreamStyleDefaultInteger"] = &xlsx.StreamStyleDefaultInteger
	decs["StreamStyleDefaultString"] = &xlsx.StreamStyleDefaultString
	decs["StreamStyleFromColumn"] = &xlsx.StreamStyleFromColumn
	decs["StreamStyleItalicInteger"] = &xlsx.StreamStyleItalicInteger
	decs["StreamStyleItalicString"] = &xlsx.StreamStyleItalicString
	decs["StreamStyleUnderlinedInteger"] = &xlsx.StreamStyleUnderlinedInteger
	decs["StreamStyleUnderlinedString"] = &xlsx.StreamStyleUnderlinedString
	decs["Style"] = reflect.TypeOf((*xlsx.Style)(nil)).Elem()
	decs["StyleInformation"] = xlsx.StyleInformation
	decs["StyleStop"] = xlsx.StyleStop
	decs["StyleWarning"] = xlsx.StyleWarning
	decs["TEMPLATE_DOCPROPS_APP"] = native.UntypedStringConst("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">\n  <TotalTime>0</TotalTime>\n  <Application>Go XLSX</Application>\n</Properties>")
	decs["TEMPLATE_DOCPROPS_CORE"] = native.UntypedStringConst("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"></cp:coreProperties>")
	decs["TEMPLATE_XL_THEME_THEME"] = native.UntypedStringConst("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<a:theme xmlns:a=\"http://schemas.openxmlformats.org/drawingml/2006/main\" name=\"Office-Design\">\n  <a:themeElements>\n    <a:clrScheme name=\"Office\">\n      <a:dk1>\n        <a:sysClr val=\"windowText\" lastClr=\"000000\"/>\n      </a:dk1>\n      <a:lt1>\n        <a:sysClr val=\"window\" lastClr=\"FFFFFF\"/>\n      </a:lt1>\n      <a:dk2>\n        <a:srgbClr val=\"1F497D\"/>\n      </a:dk2>\n      <a:lt2>\n        <a:srgbClr val=\"EEECE1\"/>\n      </a:lt2>\n      <a:accent1>\n        <a:srgbClr val=\"4F81BD\"/>\n      </a:accent1>\n      <a:accent2>\n        <a:srgbClr val=\"C0504D\"/>\n      </a:accent2>\n      <a:accent3>\n        <a:srgbClr val=\"9BBB59\"/>\n      </a:accent3>\n      <a:accent4>\n        <a:srgbClr val=\"8064A2\"/>\n      </a:accent4>\n      <a:accent5>\n        <a:srgbClr val=\"4BACC6\"/>\n      </a:accent5>\n      <a:accent6>\n        <a:srgbClr val=\"F79646\"/>\n      </a:accent6>\n      <a:hlink>\n        <a:srgbClr val=\"0000FF\"/>\n      </a:hlink>\n      <a:folHlink>\n        <a:srgbClr val=\"800080\"/>\n      </a:folHlink>\n    </a:clrScheme>\n    <a:fontScheme name=\"Office\">\n      <a:majorFont>\n        <a:latin typeface=\"Cambria\"/>\n        <a:ea typeface=\"\"/>\n        <a:cs typeface=\"\"/>\n        <a:font script=\"Jpan\" typeface=\"ＭＳ Ｐゴシック\"/>\n        <a:font script=\"Hang\" typeface=\"맑은 고딕\"/>\n        <a:font script=\"Hans\" typeface=\"宋体\"/>\n        <a:font script=\"Hant\" typeface=\"新細明體\"/>\n        <a:font script=\"Arab\" typeface=\"Times New Roman\"/>\n        <a:font script=\"Hebr\" typeface=\"Times New Roman\"/>\n        <a:font script=\"Thai\" typeface=\"Tahoma\"/>\n        <a:font script=\"Ethi\" typeface=\"Nyala\"/>\n        <a:font script=\"Beng\" typeface=\"Vrinda\"/>\n        <a:font script=\"Gujr\" typeface=\"Shruti\"/>\n        <a:font script=\"Khmr\" typeface=\"MoolBoran\"/>\n        <a:font script=\"Knda\" typeface=\"Tunga\"/>\n        <a:font script=\"Guru\" typeface=\"Raavi\"/>\n        <a:font script=\"Cans\" typeface=\"Euphemia\"/>\n        <a:font script=\"Cher\" typeface=\"Plantagenet Cherokee\"/>\n        <a:font script=\"Yiii\" typeface=\"Microsoft Yi Baiti\"/>\n        <a:font script=\"Tibt\" typeface=\"Microsoft Himalaya\"/>\n        <a:font script=\"Thaa\" typeface=\"MV Boli\"/>\n        <a:font script=\"Deva\" typeface=\"Mangal\"/>\n        <a:font script=\"Telu\" typeface=\"Gautami\"/>\n        <a:font script=\"Taml\" typeface=\"Latha\"/>\n        <a:font script=\"Syrc\" typeface=\"Estrangelo Edessa\"/>\n        <a:font script=\"Orya\" typeface=\"Kalinga\"/>\n        <a:font script=\"Mlym\" typeface=\"Kartika\"/>\n        <a:font script=\"Laoo\" typeface=\"DokChampa\"/>\n        <a:font script=\"Sinh\" typeface=\"Iskoola Pota\"/>\n        <a:font script=\"Mong\" typeface=\"Mongolian Baiti\"/>\n        <a:font script=\"Viet\" typeface=\"Times New Roman\"/>\n        <a:font script=\"Uigh\" typeface=\"Microsoft Uighur\"/>\n        <a:font script=\"Geor\" typeface=\"Sylfaen\"/>\n      </a:majorFont>\n      <a:minorFont>\n        <a:latin typeface=\"Arial\"/>\n        <a:ea typeface=\"\"/>\n        <a:cs typeface=\"\"/>\n        <a:font script=\"Jpan\" typeface=\"ＭＳ Ｐゴシック\"/>\n        <a:font script=\"Hang\" typeface=\"맑은 고딕\"/>\n        <a:font script=\"Hans\" typeface=\"宋体\"/>\n        <a:font script=\"Hant\" typeface=\"新細明體\"/>\n        <a:font script=\"Arab\" typeface=\"Arial\"/>\n        <a:font script=\"Hebr\" typeface=\"Arial\"/>\n        <a:font script=\"Thai\" typeface=\"Tahoma\"/>\n        <a:font script=\"Ethi\" typeface=\"Nyala\"/>\n        <a:font script=\"Beng\" typeface=\"Vrinda\"/>\n        <a:font script=\"Gujr\" typeface=\"Shruti\"/>\n        <a:font script=\"Khmr\" typeface=\"DaunPenh\"/>\n        <a:font script=\"Knda\" typeface=\"Tunga\"/>\n        <a:font script=\"Guru\" typeface=\"Raavi\"/>\n        <a:font script=\"Cans\" typeface=\"Euphemia\"/>\n        <a:font script=\"Cher\" typeface=\"Plantagenet Cherokee\"/>\n        <a:font script=\"Yiii\" typeface=\"Microsoft Yi Baiti\"/>\n        <a:font script=\"Tibt\" typeface=\"Microsoft Himalaya\"/>\n        <a:font script=\"Thaa\" typeface=\"MV Boli\"/>\n        <a:font script=\"Deva\" typeface=\"Mangal\"/>\n        <a:font script=\"Telu\" typeface=\"Gautami\"/>\n        <a:font script=\"Taml\" typeface=\"Latha\"/>\n        <a:font script=\"Syrc\" typeface=\"Estrangelo Edessa\"/>\n        <a:font script=\"Orya\" typeface=\"Kalinga\"/>\n        <a:font script=\"Mlym\" typeface=\"Kartika\"/>\n        <a:font script=\"Laoo\" typeface=\"DokChampa\"/>\n        <a:font script=\"Sinh\" typeface=\"Iskoola Pota\"/>\n        <a:font script=\"Mong\" typeface=\"Mongolian Baiti\"/>\n        <a:font script=\"Viet\" typeface=\"Arial\"/>\n        <a:font script=\"Uigh\" typeface=\"Microsoft Uighur\"/>\n        <a:font script=\"Geor\" typeface=\"Sylfaen\"/>\n      </a:minorFont>\n    </a:fontScheme>\n    <a:fmtScheme name=\"Office\">\n      <a:fillStyleLst>\n        <a:solidFill>\n          <a:schemeClr val=\"phClr\"/>\n        </a:solidFill>\n        <a:gradFill rotWithShape=\"1\">\n          <a:gsLst>\n            <a:gs pos=\"0\">\n              <a:schemeClr val=\"phClr\">\n                <a:tint val=\"50000\"/>\n                <a:satMod val=\"300000\"/>\n              </a:schemeClr>\n            </a:gs>\n            <a:gs pos=\"35000\">\n              <a:schemeClr val=\"phClr\">\n                <a:tint val=\"37000\"/>\n                <a:satMod val=\"300000\"/>\n              </a:schemeClr>\n            </a:gs>\n            <a:gs pos=\"100000\">\n              <a:schemeClr val=\"phClr\">\n                <a:tint val=\"15000\"/>\n                <a:satMod val=\"350000\"/>\n              </a:schemeClr>\n            </a:gs>\n          </a:gsLst>\n          <a:lin ang=\"16200000\" scaled=\"1\"/>\n        </a:gradFill>\n        <a:gradFill rotWithShape=\"1\">\n          <a:gsLst>\n            <a:gs pos=\"0\">\n              <a:schemeClr val=\"phClr\">\n                <a:tint val=\"100000\"/>\n                <a:shade val=\"100000\"/>\n                <a:satMod val=\"130000\"/>\n              </a:schemeClr>\n            </a:gs>\n            <a:gs pos=\"100000\">\n              <a:schemeClr val=\"phClr\">\n                <a:tint val=\"50000\"/>\n                <a:shade val=\"100000\"/>\n                <a:satMod val=\"350000\"/>\n              </a:schemeClr>\n            </a:gs>\n          </a:gsLst>\n          <a:lin ang=\"16200000\" scaled=\"0\"/>\n        </a:gradFill>\n      </a:fillStyleLst>\n      <a:lnStyleLst>\n        <a:ln w=\"9525\" cap=\"flat\" cmpd=\"sng\" algn=\"ctr\">\n          <a:solidFill>\n            <a:schemeClr val=\"phClr\">\n              <a:shade val=\"95000\"/>\n              <a:satMod val=\"105000\"/>\n            </a:schemeClr>\n          </a:solidFill>\n          <a:prstDash val=\"solid\"/>\n        </a:ln>\n        <a:ln w=\"25400\" cap=\"flat\" cmpd=\"sng\" algn=\"ctr\">\n          <a:solidFill>\n            <a:schemeClr val=\"phClr\"/>\n          </a:solidFill>\n          <a:prstDash val=\"solid\"/>\n        </a:ln>\n        <a:ln w=\"38100\" cap=\"flat\" cmpd=\"sng\" algn=\"ctr\">\n          <a:solidFill>\n            <a:schemeClr val=\"phClr\"/>\n          </a:solidFill>\n          <a:prstDash val=\"solid\"/>\n        </a:ln>\n      </a:lnStyleLst>\n      <a:effectStyleLst>\n        <a:effectStyle>\n          <a:effectLst>\n            <a:outerShdw blurRad=\"40000\" dist=\"20000\" dir=\"5400000\" rotWithShape=\"0\">\n              <a:srgbClr val=\"000000\">\n                <a:alpha val=\"38000\"/>\n              </a:srgbClr>\n            </a:outerShdw>\n          </a:effectLst>\n        </a:effectStyle>\n        <a:effectStyle>\n          <a:effectLst>\n            <a:outerShdw blurRad=\"40000\" dist=\"23000\" dir=\"5400000\" rotWithShape=\"0\">\n              <a:srgbClr val=\"000000\">\n                <a:alpha val=\"35000\"/>\n              </a:srgbClr>\n            </a:outerShdw>\n          </a:effectLst>\n        </a:effectStyle>\n        <a:effectStyle>\n          <a:effectLst>\n            <a:outerShdw blurRad=\"40000\" dist=\"23000\" dir=\"5400000\" rotWithShape=\"0\">\n              <a:srgbClr val=\"000000\">\n                <a:alpha val=\"35000\"/>\n              </a:srgbClr>\n            </a:outerShdw>\n          </a:effectLst>\n          <a:scene3d>\n            <a:camera prst=\"orthographicFront\">\n              <a:rot lat=\"0\" lon=\"0\" rev=\"0\"/>\n            </a:camera>\n            <a:lightRig rig=\"threePt\" dir=\"t\">\n              <a:rot lat=\"0\" lon=\"0\" rev=\"1200000\"/>\n            </a:lightRig>\n          </a:scene3d>\n          <a:sp3d>\n            <a:bevelT w=\"63500\" h=\"25400\"/>\n          </a:sp3d>\n        </a:effectStyle>\n      </a:effectStyleLst>\n      <a:bgFillStyleLst>\n        <a:solidFill>\n          <a:schemeClr val=\"phClr\"/>\n        </a:solidFill>\n        <a:gradFill rotWithShape=\"1\">\n          <a:gsLst>\n            <a:gs pos=\"0\">\n              <a:schemeClr val=\"phClr\">\n                <a:tint val=\"40000\"/>\n                <a:satMod val=\"350000\"/>\n              </a:schemeClr>\n            </a:gs>\n            <a:gs pos=\"40000\">\n              <a:schemeClr val=\"phClr\">\n                <a:tint val=\"45000\"/>\n                <a:shade val=\"99000\"/>\n                <a:satMod val=\"350000\"/>\n              </a:schemeClr>\n            </a:gs>\n            <a:gs pos=\"100000\">\n              <a:schemeClr val=\"phClr\">\n                <a:shade val=\"20000\"/>\n                <a:satMod val=\"255000\"/>\n              </a:schemeClr>\n            </a:gs>\n          </a:gsLst>\n          <a:path path=\"circle\">\n            <a:fillToRect l=\"50000\" t=\"-80000\" r=\"50000\" b=\"180000\"/>\n          </a:path>\n        </a:gradFill>\n        <a:gradFill rotWithShape=\"1\">\n          <a:gsLst>\n            <a:gs pos=\"0\">\n              <a:schemeClr val=\"phClr\">\n                <a:tint val=\"80000\"/>\n                <a:satMod val=\"300000\"/>\n              </a:schemeClr>\n            </a:gs>\n            <a:gs pos=\"100000\">\n              <a:schemeClr val=\"phClr\">\n                <a:shade val=\"30000\"/>\n                <a:satMod val=\"200000\"/>\n              </a:schemeClr>\n            </a:gs>\n          </a:gsLst>\n          <a:path path=\"circle\">\n            <a:fillToRect l=\"50000\" t=\"50000\" r=\"50000\" b=\"50000\"/>\n          </a:path>\n        </a:gradFill>\n      </a:bgFillStyleLst>\n    </a:fmtScheme>\n  </a:themeElements>\n  <a:objectDefaults>\n    <a:spDef>\n      <a:spPr/>\n      <a:bodyPr/>\n      <a:lstStyle/>\n      <a:style>\n        <a:lnRef idx=\"1\">\n          <a:schemeClr val=\"accent1\"/>\n        </a:lnRef>\n        <a:fillRef idx=\"3\">\n          <a:schemeClr val=\"accent1\"/>\n        </a:fillRef>\n        <a:effectRef idx=\"2\">\n          <a:schemeClr val=\"accent1\"/>\n        </a:effectRef>\n        <a:fontRef idx=\"minor\">\n          <a:schemeClr val=\"lt1\"/>\n        </a:fontRef>\n      </a:style>\n    </a:spDef>\n    <a:lnDef>\n      <a:spPr/>\n      <a:bodyPr/>\n      <a:lstStyle/>\n      <a:style>\n        <a:lnRef idx=\"2\">\n          <a:schemeClr val=\"accent1\"/>\n        </a:lnRef>\n        <a:fillRef idx=\"0\">\n          <a:schemeClr val=\"accent1\"/>\n        </a:fillRef>\n        <a:effectRef idx=\"1\">\n          <a:schemeClr val=\"accent1\"/>\n        </a:effectRef>\n        <a:fontRef idx=\"minor\">\n          <a:schemeClr val=\"tx1\"/>\n        </a:fontRef>\n      </a:style>\n    </a:lnDef>\n  </a:objectDefaults>\n  <a:extraClrSchemeLst/>\n</a:theme>")
	decs["TEMPLATE__RELS_DOT_RELS"] = native.UntypedStringConst("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\n  <Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>\n  <Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" Target=\"docProps/core.xml\"/>\n  <Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" Target=\"docProps/app.xml\"/>\n</Relationships>")
	decs["TimeFromExcelTime"] = xlsx.TimeFromExcelTime
	decs["TimeToExcelTime"] = xlsx.TimeToExcelTime
	decs["TimeToUTCTime"] = xlsx.TimeToUTCTime
	decs["TimesNewRoman"] = native.UntypedStringConst("Times New Roman")
	decs["UnsupportedCellTypeError"] = &xlsx.UnsupportedCellTypeError
	decs["WorkBookRels"] = reflect.TypeOf((*xlsx.WorkBookRels)(nil)).Elem()
	decs["WrongNumberOfRowsError"] = &xlsx.WrongNumberOfRowsError
	decs["XLSXReaderError"] = reflect.TypeOf((*xlsx.XLSXReaderError)(nil)).Elem()
	decs["XLSXUnmarshaler"] = reflect.TypeOf((*xlsx.XLSXUnmarshaler)(nil)).Elem()
	packages["github.com/tealeg/xlsx"] = native.Package{
		Name:         "xlsx",
		Declarations: decs,
	}
	// "go/ast"
	decs = make(native.Declarations, 100)
	decs["ArrayType"] = reflect.TypeOf((*ast.ArrayType)(nil)).Elem()
	decs["AssignStmt"] = reflect.TypeOf((*ast.AssignStmt)(nil)).Elem()
	decs["Bad"] = ast.Bad
	decs["BadDecl"] = reflect.TypeOf((*ast.BadDecl)(nil)).Elem()
	decs["BadExpr"] = reflect.TypeOf((*ast.BadExpr)(nil)).Elem()
	decs["BadStmt"] = reflect.TypeOf((*ast.BadStmt)(nil)).Elem()
	decs["BasicLit"] = reflect.TypeOf((*ast.BasicLit)(nil)).Elem()
	decs["BinaryExpr"] = reflect.TypeOf((*ast.BinaryExpr)(nil)).Elem()
	decs["BlockStmt"] = reflect.TypeOf((*ast.BlockStmt)(nil)).Elem()
	decs["BranchStmt"] = reflect.TypeOf((*ast.BranchStmt)(nil)).Elem()
	decs["CallExpr"] = reflect.TypeOf((*ast.CallExpr)(nil)).Elem()
	decs["CaseClause"] = reflect.TypeOf((*ast.CaseClause)(nil)).Elem()
	decs["ChanDir"] = reflect.TypeOf((*ast.ChanDir)(nil)).Elem()
	decs["ChanType"] = reflect.TypeOf((*ast.ChanType)(nil)).Elem()
	decs["CommClause"] = reflect.TypeOf((*ast.CommClause)(nil)).Elem()
	decs["Comment"] = reflect.TypeOf((*ast.Comment)(nil)).Elem()
	decs["CommentGroup"] = reflect.TypeOf((*ast.CommentGroup)(nil)).Elem()
	decs["CommentMap"] = reflect.TypeOf((*ast.CommentMap)(nil)).Elem()
	decs["CompositeLit"] = reflect.TypeOf((*ast.CompositeLit)(nil)).Elem()
	decs["Con"] = ast.Con
	decs["Decl"] = reflect.TypeOf((*ast.Decl)(nil)).Elem()
	decs["DeclStmt"] = reflect.TypeOf((*ast.DeclStmt)(nil)).Elem()
	decs["DeferStmt"] = reflect.TypeOf((*ast.DeferStmt)(nil)).Elem()
	decs["Ellipsis"] = reflect.TypeOf((*ast.Ellipsis)(nil)).Elem()
	decs["EmptyStmt"] = reflect.TypeOf((*ast.EmptyStmt)(nil)).Elem()
	decs["Expr"] = reflect.TypeOf((*ast.Expr)(nil)).Elem()
	decs["ExprStmt"] = reflect.TypeOf((*ast.ExprStmt)(nil)).Elem()
	decs["Field"] = reflect.TypeOf((*ast.Field)(nil)).Elem()
	decs["FieldFilter"] = reflect.TypeOf((*ast.FieldFilter)(nil)).Elem()
	decs["FieldList"] = reflect.TypeOf((*ast.FieldList)(nil)).Elem()
	decs["File"] = reflect.TypeOf((*ast.File)(nil)).Elem()
	decs["FileExports"] = ast.FileExports
	decs["Filter"] = reflect.TypeOf((*ast.Filter)(nil)).Elem()
	decs["FilterDecl"] = ast.FilterDecl
	decs["FilterFile"] = ast.FilterFile
	decs["FilterFuncDuplicates"] = ast.FilterFuncDuplicates
	decs["FilterImportDuplicates"] = ast.FilterImportDuplicates
	decs["FilterPackage"] = ast.FilterPackage
	decs["FilterUnassociatedComments"] = ast.FilterUnassociatedComments
	decs["ForStmt"] = reflect.TypeOf((*ast.ForStmt)(nil)).Elem()
	decs["Fprint"] = ast.Fprint
	decs["Fun"] = ast.Fun
	decs["FuncDecl"] = reflect.TypeOf((*ast.FuncDecl)(nil)).Elem()
	decs["FuncLit"] = reflect.TypeOf((*ast.FuncLit)(nil)).Elem()
	decs["FuncType"] = reflect.TypeOf((*ast.FuncType)(nil)).Elem()
	decs["GenDecl"] = reflect.TypeOf((*ast.GenDecl)(nil)).Elem()
	decs["GoStmt"] = reflect.TypeOf((*ast.GoStmt)(nil)).Elem()
	decs["Ident"] = reflect.TypeOf((*ast.Ident)(nil)).Elem()
	decs["IfStmt"] = reflect.TypeOf((*ast.IfStmt)(nil)).Elem()
	decs["ImportSpec"] = reflect.TypeOf((*ast.ImportSpec)(nil)).Elem()
	decs["Importer"] = reflect.TypeOf((*ast.Importer)(nil)).Elem()
	decs["IncDecStmt"] = reflect.TypeOf((*ast.IncDecStmt)(nil)).Elem()
	decs["IndexExpr"] = reflect.TypeOf((*ast.IndexExpr)(nil)).Elem()
	decs["Inspect"] = ast.Inspect
	decs["InterfaceType"] = reflect.TypeOf((*ast.InterfaceType)(nil)).Elem()
	decs["IsExported"] = ast.IsExported
	decs["KeyValueExpr"] = reflect.TypeOf((*ast.KeyValueExpr)(nil)).Elem()
	decs["LabeledStmt"] = reflect.TypeOf((*ast.LabeledStmt)(nil)).Elem()
	decs["Lbl"] = ast.Lbl
	decs["MapType"] = reflect.TypeOf((*ast.MapType)(nil)).Elem()
	decs["MergeMode"] = reflect.TypeOf((*ast.MergeMode)(nil)).Elem()
	decs["MergePackageFiles"] = ast.MergePackageFiles
	decs["NewCommentMap"] = ast.NewCommentMap
	decs["NewIdent"] = ast.NewIdent
	decs["NewObj"] = ast.NewObj
	decs["NewPackage"] = ast.NewPackage
	decs["NewScope"] = ast.NewScope
	decs["Node"] = reflect.TypeOf((*ast.Node)(nil)).Elem()
	decs["NotNilFilter"] = ast.NotNilFilter
	decs["ObjKind"] = reflect.TypeOf((*ast.ObjKind)(nil)).Elem()
	decs["Object"] = reflect.TypeOf((*ast.Object)(nil)).Elem()
	decs["Package"] = reflect.TypeOf((*ast.Package)(nil)).Elem()
	decs["PackageExports"] = ast.PackageExports
	decs["ParenExpr"] = reflect.TypeOf((*ast.ParenExpr)(nil)).Elem()
	decs["Pkg"] = ast.Pkg
	decs["Print"] = ast.Print
	decs["RECV"] = ast.RECV
	decs["RangeStmt"] = reflect.TypeOf((*ast.RangeStmt)(nil)).Elem()
	decs["ReturnStmt"] = reflect.TypeOf((*ast.ReturnStmt)(nil)).Elem()
	decs["SEND"] = ast.SEND
	decs["Scope"] = reflect.TypeOf((*ast.Scope)(nil)).Elem()
	decs["SelectStmt"] = reflect.TypeOf((*ast.SelectStmt)(nil)).Elem()
	decs["SelectorExpr"] = reflect.TypeOf((*ast.SelectorExpr)(nil)).Elem()
	decs["SendStmt"] = reflect.TypeOf((*ast.SendStmt)(nil)).Elem()
	decs["SliceExpr"] = reflect.TypeOf((*ast.SliceExpr)(nil)).Elem()
	decs["SortImports"] = ast.SortImports
	decs["Spec"] = reflect.TypeOf((*ast.Spec)(nil)).Elem()
	decs["StarExpr"] = reflect.TypeOf((*ast.StarExpr)(nil)).Elem()
	decs["Stmt"] = reflect.TypeOf((*ast.Stmt)(nil)).Elem()
	decs["StructType"] = reflect.TypeOf((*ast.StructType)(nil)).Elem()
	decs["SwitchStmt"] = reflect.TypeOf((*ast.SwitchStmt)(nil)).Elem()
	decs["Typ"] = ast.Typ
	decs["TypeAssertExpr"] = reflect.TypeOf((*ast.TypeAssertExpr)(nil)).Elem()
	decs["TypeSpec"] = reflect.TypeOf((*ast.TypeSpec)(nil)).Elem()
	decs["TypeSwitchStmt"] = reflect.TypeOf((*ast.TypeSwitchStmt)(nil)).Elem()
	decs["UnaryExpr"] = reflect.TypeOf((*ast.UnaryExpr)(nil)).Elem()
	decs["ValueSpec"] = reflect.TypeOf((*ast.ValueSpec)(nil)).Elem()
	decs["Var"] = ast.Var
	decs["Visitor"] = reflect.TypeOf((*ast.Visitor)(nil)).Elem()
	decs["Walk"] = ast.Walk
	packages["go/ast"] = native.Package{
		Name:         "ast",
		Declarations: decs,
	}
	// "go/build"
	decs = make(native.Declarations, 15)
	decs["AllowBinary"] = build.AllowBinary
	decs["ArchChar"] = build.ArchChar
	decs["Context"] = reflect.TypeOf((*build.Context)(nil)).Elem()
	decs["Default"] = &build.Default
	decs["FindOnly"] = build.FindOnly
	decs["IgnoreVendor"] = build.IgnoreVendor
	decs["Import"] = build.Import
	decs["ImportComment"] = build.ImportComment
	decs["ImportDir"] = build.ImportDir
	decs["ImportMode"] = reflect.TypeOf((*build.ImportMode)(nil)).Elem()
	decs["IsLocalImport"] = build.IsLocalImport
	decs["MultiplePackageError"] = reflect.TypeOf((*build.MultiplePackageError)(nil)).Elem()
	decs["NoGoError"] = reflect.TypeOf((*build.NoGoError)(nil)).Elem()
	decs["Package"] = reflect.TypeOf((*build.Package)(nil)).Elem()
	decs["ToolDir"] = &build.ToolDir
	packages["go/build"] = native.Package{
		Name:         "build",
		Declarations: decs,
	}
	// "go/constant"
	decs = make(native.Declarations, 39)
	decs["BinaryOp"] = constant.BinaryOp
	decs["BitLen"] = constant.BitLen
	decs["Bool"] = constant.Bool
	decs["BoolVal"] = constant.BoolVal
	decs["Bytes"] = constant.Bytes
	decs["Compare"] = constant.Compare
	decs["Complex"] = constant.Complex
	decs["Denom"] = constant.Denom
	decs["Float"] = constant.Float
	decs["Float32Val"] = constant.Float32Val
	decs["Float64Val"] = constant.Float64Val
	decs["Imag"] = constant.Imag
	decs["Int"] = constant.Int
	decs["Int64Val"] = constant.Int64Val
	decs["Kind"] = reflect.TypeOf((*constant.Kind)(nil)).Elem()
	decs["Make"] = constant.Make
	decs["MakeBool"] = constant.MakeBool
	decs["MakeFloat64"] = constant.MakeFloat64
	decs["MakeFromBytes"] = constant.MakeFromBytes
	decs["MakeFromLiteral"] = constant.MakeFromLiteral
	decs["MakeImag"] = constant.MakeImag
	decs["MakeInt64"] = constant.MakeInt64
	decs["MakeString"] = constant.MakeString
	decs["MakeUint64"] = constant.MakeUint64
	decs["MakeUnknown"] = constant.MakeUnknown
	decs["Num"] = constant.Num
	decs["Real"] = constant.Real
	decs["Shift"] = constant.Shift
	decs["Sign"] = constant.Sign
	decs["String"] = constant.String
	decs["StringVal"] = constant.StringVal
	decs["ToComplex"] = constant.ToComplex
	decs["ToFloat"] = constant.ToFloat
	decs["ToInt"] = constant.ToInt
	decs["Uint64Val"] = constant.Uint64Val
	decs["UnaryOp"] = constant.UnaryOp
	decs["Unknown"] = constant.Unknown
	decs["Val"] = constant.Val
	decs["Value"] = reflect.TypeOf((*constant.Value)(nil)).Elem()
	packages["go/constant"] = native.Package{
		Name:         "constant",
		Declarations: decs,
	}
	// "go/doc"
	decs = make(native.Declarations, 19)
	decs["AllDecls"] = doc.AllDecls
	decs["AllMethods"] = doc.AllMethods
	decs["Example"] = reflect.TypeOf((*doc.Example)(nil)).Elem()
	decs["Examples"] = doc.Examples
	decs["Filter"] = reflect.TypeOf((*doc.Filter)(nil)).Elem()
	decs["Func"] = reflect.TypeOf((*doc.Func)(nil)).Elem()
	decs["IllegalPrefixes"] = &doc.IllegalPrefixes
	decs["IsPredeclared"] = doc.IsPredeclared
	decs["Mode"] = reflect.TypeOf((*doc.Mode)(nil)).Elem()
	decs["New"] = doc.New
	decs["NewFromFiles"] = doc.NewFromFiles
	decs["Note"] = reflect.TypeOf((*doc.Note)(nil)).Elem()
	decs["Package"] = reflect.TypeOf((*doc.Package)(nil)).Elem()
	decs["PreserveAST"] = doc.PreserveAST
	decs["Synopsis"] = doc.Synopsis
	decs["ToHTML"] = doc.ToHTML
	decs["ToText"] = doc.ToText
	decs["Type"] = reflect.TypeOf((*doc.Type)(nil)).Elem()
	decs["Value"] = reflect.TypeOf((*doc.Value)(nil)).Elem()
	packages["go/doc"] = native.Package{
		Name:         "doc",
		Declarations: decs,
	}
	// "go/format"
	decs = make(native.Declarations, 2)
	decs["Node"] = format.Node
	decs["Source"] = format.Source
	packages["go/format"] = native.Package{
		Name:         "format",
		Declarations: decs,
	}
	// "go/importer"
	decs = make(native.Declarations, 4)
	decs["Default"] = importer.Default
	decs["For"] = importer.For
	decs["ForCompiler"] = importer.ForCompiler
	decs["Lookup"] = reflect.TypeOf((*importer.Lookup)(nil)).Elem()
	packages["go/importer"] = native.Package{
		Name:         "importer",
		Declarations: decs,
	}
	// "go/parser"
	decs = make(native.Declarations, 12)
	decs["AllErrors"] = parser.AllErrors
	decs["DeclarationErrors"] = parser.DeclarationErrors
	decs["ImportsOnly"] = parser.ImportsOnly
	decs["Mode"] = reflect.TypeOf((*parser.Mode)(nil)).Elem()
	decs["PackageClauseOnly"] = parser.PackageClauseOnly
	decs["ParseComments"] = parser.ParseComments
	decs["ParseDir"] = parser.ParseDir
	decs["ParseExpr"] = parser.ParseExpr
	decs["ParseExprFrom"] = parser.ParseExprFrom
	decs["ParseFile"] = parser.ParseFile
	decs["SpuriousErrors"] = parser.SpuriousErrors
	decs["Trace"] = parser.Trace
	packages["go/parser"] = native.Package{
		Name:         "parser",
		Declarations: decs,
	}
	// "go/printer"
	decs = make(native.Declarations, 8)
	decs["CommentedNode"] = reflect.TypeOf((*printer.CommentedNode)(nil)).Elem()
	decs["Config"] = reflect.TypeOf((*printer.Config)(nil)).Elem()
	decs["Fprint"] = printer.Fprint
	decs["Mode"] = reflect.TypeOf((*printer.Mode)(nil)).Elem()
	decs["RawFormat"] = printer.RawFormat
	decs["SourcePos"] = printer.SourcePos
	decs["TabIndent"] = printer.TabIndent
	decs["UseSpaces"] = printer.UseSpaces
	packages["go/printer"] = native.Package{
		Name:         "printer",
		Declarations: decs,
	}
	// "go/scanner"
	decs = make(native.Declarations, 7)
	decs["Error"] = reflect.TypeOf((*scanner.Error)(nil)).Elem()
	decs["ErrorHandler"] = reflect.TypeOf((*scanner.ErrorHandler)(nil)).Elem()
	decs["ErrorList"] = reflect.TypeOf((*scanner.ErrorList)(nil)).Elem()
	decs["Mode"] = reflect.TypeOf((*scanner.Mode)(nil)).Elem()
	decs["PrintError"] = scanner.PrintError
	decs["ScanComments"] = scanner.ScanComments
	decs["Scanner"] = reflect.TypeOf((*scanner.Scanner)(nil)).Elem()
	packages["go/scanner"] = native.Package{
		Name:         "scanner",
		Declarations: decs,
	}
	// "go/token"
	decs = make(native.Declarations, 95)
	decs["ADD"] = token.ADD
	decs["ADD_ASSIGN"] = token.ADD_ASSIGN
	decs["AND"] = token.AND
	decs["AND_ASSIGN"] = token.AND_ASSIGN
	decs["AND_NOT"] = token.AND_NOT
	decs["AND_NOT_ASSIGN"] = token.AND_NOT_ASSIGN
	decs["ARROW"] = token.ARROW
	decs["ASSIGN"] = token.ASSIGN
	decs["BREAK"] = token.BREAK
	decs["CASE"] = token.CASE
	decs["CHAN"] = token.CHAN
	decs["CHAR"] = token.CHAR
	decs["COLON"] = token.COLON
	decs["COMMA"] = token.COMMA
	decs["COMMENT"] = token.COMMENT
	decs["CONST"] = token.CONST
	decs["CONTINUE"] = token.CONTINUE
	decs["DEC"] = token.DEC
	decs["DEFAULT"] = token.DEFAULT
	decs["DEFER"] = token.DEFER
	decs["DEFINE"] = token.DEFINE
	decs["ELLIPSIS"] = token.ELLIPSIS
	decs["ELSE"] = token.ELSE
	decs["EOF"] = token.EOF
	decs["EQL"] = token.EQL
	decs["FALLTHROUGH"] = token.FALLTHROUGH
	decs["FLOAT"] = token.FLOAT
	decs["FOR"] = token.FOR
	decs["FUNC"] = token.FUNC
	decs["File"] = reflect.TypeOf((*token.File)(nil)).Elem()
	decs["FileSet"] = reflect.TypeOf((*token.FileSet)(nil)).Elem()
	decs["GEQ"] = token.GEQ
	decs["GO"] = token.GO
	decs["GOTO"] = token.GOTO
	decs["GTR"] = token.GTR
	decs["HighestPrec"] = native.UntypedNumericConst("7")
	decs["IDENT"] = token.IDENT
	decs["IF"] = token.IF
	decs["ILLEGAL"] = token.ILLEGAL
	decs["IMAG"] = token.IMAG
	decs["IMPORT"] = token.IMPORT
	decs["INC"] = token.INC
	decs["INT"] = token.INT
	decs["INTERFACE"] = token.INTERFACE
	decs["IsExported"] = token.IsExported
	decs["IsIdentifier"] = token.IsIdentifier
	decs["IsKeyword"] = token.IsKeyword
	decs["LAND"] = token.LAND
	decs["LBRACE"] = token.LBRACE
	decs["LBRACK"] = token.LBRACK
	decs["LEQ"] = token.LEQ
	decs["LOR"] = token.LOR
	decs["LPAREN"] = token.LPAREN
	decs["LSS"] = token.LSS
	decs["Lookup"] = token.Lookup
	decs["LowestPrec"] = native.UntypedNumericConst("0")
	decs["MAP"] = token.MAP
	decs["MUL"] = token.MUL
	decs["MUL_ASSIGN"] = token.MUL_ASSIGN
	decs["NEQ"] = token.NEQ
	decs["NOT"] = token.NOT
	decs["NewFileSet"] = token.NewFileSet
	decs["NoPos"] = token.NoPos
	decs["OR"] = token.OR
	decs["OR_ASSIGN"] = token.OR_ASSIGN
	decs["PACKAGE"] = token.PACKAGE
	decs["PERIOD"] = token.PERIOD
	decs["Pos"] = reflect.TypeOf((*token.Pos)(nil)).Elem()
	decs["Position"] = reflect.TypeOf((*token.Position)(nil)).Elem()
	decs["QUO"] = token.QUO
	decs["QUO_ASSIGN"] = token.QUO_ASSIGN
	decs["RANGE"] = token.RANGE
	decs["RBRACE"] = token.RBRACE
	decs["RBRACK"] = token.RBRACK
	decs["REM"] = token.REM
	decs["REM_ASSIGN"] = token.REM_ASSIGN
	decs["RETURN"] = token.RETURN
	decs["RPAREN"] = token.RPAREN
	decs["SELECT"] = token.SELECT
	decs["SEMICOLON"] = token.SEMICOLON
	decs["SHL"] = token.SHL
	decs["SHL_ASSIGN"] = token.SHL_ASSIGN
	decs["SHR"] = token.SHR
	decs["SHR_ASSIGN"] = token.SHR_ASSIGN
	decs["STRING"] = token.STRING
	decs["STRUCT"] = token.STRUCT
	decs["SUB"] = token.SUB
	decs["SUB_ASSIGN"] = token.SUB_ASSIGN
	decs["SWITCH"] = token.SWITCH
	decs["TYPE"] = token.TYPE
	decs["Token"] = reflect.TypeOf((*token.Token)(nil)).Elem()
	decs["UnaryPrec"] = native.UntypedNumericConst("6")
	decs["VAR"] = token.VAR
	decs["XOR"] = token.XOR
	decs["XOR_ASSIGN"] = token.XOR_ASSIGN
	packages["go/token"] = native.Package{
		Name:         "token",
		Declarations: decs,
	}
	// "go/types"
	decs = make(native.Declarations, 135)
	decs["Array"] = reflect.TypeOf((*types.Array)(nil)).Elem()
	decs["AssertableTo"] = types.AssertableTo
	decs["AssignableTo"] = types.AssignableTo
	decs["Basic"] = reflect.TypeOf((*types.Basic)(nil)).Elem()
	decs["BasicInfo"] = reflect.TypeOf((*types.BasicInfo)(nil)).Elem()
	decs["BasicKind"] = reflect.TypeOf((*types.BasicKind)(nil)).Elem()
	decs["Bool"] = types.Bool
	decs["Builtin"] = reflect.TypeOf((*types.Builtin)(nil)).Elem()
	decs["Byte"] = types.Byte
	decs["Chan"] = reflect.TypeOf((*types.Chan)(nil)).Elem()
	decs["ChanDir"] = reflect.TypeOf((*types.ChanDir)(nil)).Elem()
	decs["CheckExpr"] = types.CheckExpr
	decs["Checker"] = reflect.TypeOf((*types.Checker)(nil)).Elem()
	decs["Comparable"] = types.Comparable
	decs["Complex128"] = types.Complex128
	decs["Complex64"] = types.Complex64
	decs["Config"] = reflect.TypeOf((*types.Config)(nil)).Elem()
	decs["Const"] = reflect.TypeOf((*types.Const)(nil)).Elem()
	decs["ConvertibleTo"] = types.ConvertibleTo
	decs["DefPredeclaredTestFuncs"] = types.DefPredeclaredTestFuncs
	decs["Default"] = types.Default
	decs["Error"] = reflect.TypeOf((*types.Error)(nil)).Elem()
	decs["Eval"] = types.Eval
	decs["ExprString"] = types.ExprString
	decs["FieldVal"] = types.FieldVal
	decs["Float32"] = types.Float32
	decs["Float64"] = types.Float64
	decs["Func"] = reflect.TypeOf((*types.Func)(nil)).Elem()
	decs["Id"] = types.Id
	decs["Identical"] = types.Identical
	decs["IdenticalIgnoreTags"] = types.IdenticalIgnoreTags
	decs["Implements"] = types.Implements
	decs["ImportMode"] = reflect.TypeOf((*types.ImportMode)(nil)).Elem()
	decs["Importer"] = reflect.TypeOf((*types.Importer)(nil)).Elem()
	decs["ImporterFrom"] = reflect.TypeOf((*types.ImporterFrom)(nil)).Elem()
	decs["Info"] = reflect.TypeOf((*types.Info)(nil)).Elem()
	decs["Initializer"] = reflect.TypeOf((*types.Initializer)(nil)).Elem()
	decs["Int"] = types.Int
	decs["Int16"] = types.Int16
	decs["Int32"] = types.Int32
	decs["Int64"] = types.Int64
	decs["Int8"] = types.Int8
	decs["Interface"] = reflect.TypeOf((*types.Interface)(nil)).Elem()
	decs["Invalid"] = types.Invalid
	decs["IsBoolean"] = types.IsBoolean
	decs["IsComplex"] = types.IsComplex
	decs["IsConstType"] = types.IsConstType
	decs["IsFloat"] = types.IsFloat
	decs["IsInteger"] = types.IsInteger
	decs["IsInterface"] = types.IsInterface
	decs["IsNumeric"] = types.IsNumeric
	decs["IsOrdered"] = types.IsOrdered
	decs["IsString"] = types.IsString
	decs["IsUnsigned"] = types.IsUnsigned
	decs["IsUntyped"] = types.IsUntyped
	decs["Label"] = reflect.TypeOf((*types.Label)(nil)).Elem()
	decs["LookupFieldOrMethod"] = types.LookupFieldOrMethod
	decs["Map"] = reflect.TypeOf((*types.Map)(nil)).Elem()
	decs["MethodExpr"] = types.MethodExpr
	decs["MethodSet"] = reflect.TypeOf((*types.MethodSet)(nil)).Elem()
	decs["MethodVal"] = types.MethodVal
	decs["MissingMethod"] = types.MissingMethod
	decs["Named"] = reflect.TypeOf((*types.Named)(nil)).Elem()
	decs["NewArray"] = types.NewArray
	decs["NewChan"] = types.NewChan
	decs["NewChecker"] = types.NewChecker
	decs["NewConst"] = types.NewConst
	decs["NewField"] = types.NewField
	decs["NewFunc"] = types.NewFunc
	decs["NewInterface"] = types.NewInterface
	decs["NewInterfaceType"] = types.NewInterfaceType
	decs["NewLabel"] = types.NewLabel
	decs["NewMap"] = types.NewMap
	decs["NewMethodSet"] = types.NewMethodSet
	decs["NewNamed"] = types.NewNamed
	decs["NewPackage"] = types.NewPackage
	decs["NewParam"] = types.NewParam
	decs["NewPkgName"] = types.NewPkgName
	decs["NewPointer"] = types.NewPointer
	decs["NewScope"] = types.NewScope
	decs["NewSignature"] = types.NewSignature
	decs["NewSlice"] = types.NewSlice
	decs["NewStruct"] = types.NewStruct
	decs["NewTuple"] = types.NewTuple
	decs["NewTypeName"] = types.NewTypeName
	decs["NewVar"] = types.NewVar
	decs["Nil"] = reflect.TypeOf((*types.Nil)(nil)).Elem()
	decs["Object"] = reflect.TypeOf((*types.Object)(nil)).Elem()
	decs["ObjectString"] = types.ObjectString
	decs["Package"] = reflect.TypeOf((*types.Package)(nil)).Elem()
	decs["PkgName"] = reflect.TypeOf((*types.PkgName)(nil)).Elem()
	decs["Pointer"] = reflect.TypeOf((*types.Pointer)(nil)).Elem()
	decs["Qualifier"] = reflect.TypeOf((*types.Qualifier)(nil)).Elem()
	decs["RecvOnly"] = types.RecvOnly
	decs["RelativeTo"] = types.RelativeTo
	decs["Rune"] = types.Rune
	decs["Scope"] = reflect.TypeOf((*types.Scope)(nil)).Elem()
	decs["Selection"] = reflect.TypeOf((*types.Selection)(nil)).Elem()
	decs["SelectionKind"] = reflect.TypeOf((*types.SelectionKind)(nil)).Elem()
	decs["SelectionString"] = types.SelectionString
	decs["SendOnly"] = types.SendOnly
	decs["SendRecv"] = types.SendRecv
	decs["Signature"] = reflect.TypeOf((*types.Signature)(nil)).Elem()
	decs["Sizes"] = reflect.TypeOf((*types.Sizes)(nil)).Elem()
	decs["SizesFor"] = types.SizesFor
	decs["Slice"] = reflect.TypeOf((*types.Slice)(nil)).Elem()
	decs["StdSizes"] = reflect.TypeOf((*types.StdSizes)(nil)).Elem()
	decs["String"] = types.String
	decs["Struct"] = reflect.TypeOf((*types.Struct)(nil)).Elem()
	decs["Tuple"] = reflect.TypeOf((*types.Tuple)(nil)).Elem()
	decs["Typ"] = &types.Typ
	decs["Type"] = reflect.TypeOf((*types.Type)(nil)).Elem()
	decs["TypeAndValue"] = reflect.TypeOf((*types.TypeAndValue)(nil)).Elem()
	decs["TypeName"] = reflect.TypeOf((*types.TypeName)(nil)).Elem()
	decs["TypeString"] = types.TypeString
	decs["Uint"] = types.Uint
	decs["Uint16"] = types.Uint16
	decs["Uint32"] = types.Uint32
	decs["Uint64"] = types.Uint64
	decs["Uint8"] = types.Uint8
	decs["Uintptr"] = types.Uintptr
	decs["Universe"] = &types.Universe
	decs["Unsafe"] = &types.Unsafe
	decs["UnsafePointer"] = types.UnsafePointer
	decs["UntypedBool"] = types.UntypedBool
	decs["UntypedComplex"] = types.UntypedComplex
	decs["UntypedFloat"] = types.UntypedFloat
	decs["UntypedInt"] = types.UntypedInt
	decs["UntypedNil"] = types.UntypedNil
	decs["UntypedRune"] = types.UntypedRune
	decs["UntypedString"] = types.UntypedString
	decs["Var"] = reflect.TypeOf((*types.Var)(nil)).Elem()
	decs["WriteExpr"] = types.WriteExpr
	decs["WriteSignature"] = types.WriteSignature
	decs["WriteType"] = types.WriteType
	packages["go/types"] = native.Package{
		Name:         "types",
		Declarations: decs,
	}
	// "hash"
	decs = make(native.Declarations, 3)
	decs["Hash"] = reflect.TypeOf((*hash.Hash)(nil)).Elem()
	decs["Hash32"] = reflect.TypeOf((*hash.Hash32)(nil)).Elem()
	decs["Hash64"] = reflect.TypeOf((*hash.Hash64)(nil)).Elem()
	packages["hash"] = native.Package{
		Name:         "hash",
		Declarations: decs,
	}
	// "hash/adler32"
	decs = make(native.Declarations, 3)
	decs["Checksum"] = adler32.Checksum
	decs["New"] = adler32.New
	decs["Size"] = native.UntypedNumericConst("4")
	packages["hash/adler32"] = native.Package{
		Name:         "adler32",
		Declarations: decs,
	}
	// "hash/crc32"
	decs = make(native.Declarations, 12)
	decs["Castagnoli"] = native.UntypedNumericConst("2197175160")
	decs["Checksum"] = crc32.Checksum
	decs["ChecksumIEEE"] = crc32.ChecksumIEEE
	decs["IEEE"] = native.UntypedNumericConst("3988292384")
	decs["IEEETable"] = &crc32.IEEETable
	decs["Koopman"] = native.UntypedNumericConst("3945912366")
	decs["MakeTable"] = crc32.MakeTable
	decs["New"] = crc32.New
	decs["NewIEEE"] = crc32.NewIEEE
	decs["Size"] = native.UntypedNumericConst("4")
	decs["Table"] = reflect.TypeOf((*crc32.Table)(nil)).Elem()
	decs["Update"] = crc32.Update
	packages["hash/crc32"] = native.Package{
		Name:         "crc32",
		Declarations: decs,
	}
	// "hash/crc64"
	decs = make(native.Declarations, 8)
	decs["Checksum"] = crc64.Checksum
	decs["ECMA"] = native.UntypedNumericConst("14514072000185962306")
	decs["ISO"] = native.UntypedNumericConst("15564440312192434176")
	decs["MakeTable"] = crc64.MakeTable
	decs["New"] = crc64.New
	decs["Size"] = native.UntypedNumericConst("8")
	decs["Table"] = reflect.TypeOf((*crc64.Table)(nil)).Elem()
	decs["Update"] = crc64.Update
	packages["hash/crc64"] = native.Package{
		Name:         "crc64",
		Declarations: decs,
	}
	// "hash/fnv"
	decs = make(native.Declarations, 6)
	decs["New128"] = fnv.New128
	decs["New128a"] = fnv.New128a
	decs["New32"] = fnv.New32
	decs["New32a"] = fnv.New32a
	decs["New64"] = fnv.New64
	decs["New64a"] = fnv.New64a
	packages["hash/fnv"] = native.Package{
		Name:         "fnv",
		Declarations: decs,
	}
	// "hash/maphash"
	decs = make(native.Declarations, 3)
	decs["Hash"] = reflect.TypeOf((*maphash.Hash)(nil)).Elem()
	decs["MakeSeed"] = maphash.MakeSeed
	decs["Seed"] = reflect.TypeOf((*maphash.Seed)(nil)).Elem()
	packages["hash/maphash"] = native.Package{
		Name:         "maphash",
		Declarations: decs,
	}
	// "html"
	decs = make(native.Declarations, 2)
	decs["EscapeString"] = html.EscapeString
	decs["UnescapeString"] = html.UnescapeString
	packages["html"] = native.Package{
		Name:         "html",
		Declarations: decs,
	}
	// "html/template"
	decs = make(native.Declarations, 36)
	decs["CSS"] = reflect.TypeOf((*template.CSS)(nil)).Elem()
	decs["ErrAmbigContext"] = template.ErrAmbigContext
	decs["ErrBadHTML"] = template.ErrBadHTML
	decs["ErrBranchEnd"] = template.ErrBranchEnd
	decs["ErrEndContext"] = template.ErrEndContext
	decs["ErrNoSuchTemplate"] = template.ErrNoSuchTemplate
	decs["ErrOutputContext"] = template.ErrOutputContext
	decs["ErrPartialCharset"] = template.ErrPartialCharset
	decs["ErrPartialEscape"] = template.ErrPartialEscape
	decs["ErrPredefinedEscaper"] = template.ErrPredefinedEscaper
	decs["ErrRangeLoopReentry"] = template.ErrRangeLoopReentry
	decs["ErrSlashAmbig"] = template.ErrSlashAmbig
	decs["Error"] = reflect.TypeOf((*template.Error)(nil)).Elem()
	decs["ErrorCode"] = reflect.TypeOf((*template.ErrorCode)(nil)).Elem()
	decs["FuncMap"] = reflect.TypeOf((*template.FuncMap)(nil)).Elem()
	decs["HTML"] = reflect.TypeOf((*template.HTML)(nil)).Elem()
	decs["HTMLAttr"] = reflect.TypeOf((*template.HTMLAttr)(nil)).Elem()
	decs["HTMLEscape"] = template.HTMLEscape
	decs["HTMLEscapeString"] = template.HTMLEscapeString
	decs["HTMLEscaper"] = template.HTMLEscaper
	decs["IsTrue"] = template.IsTrue
	decs["JS"] = reflect.TypeOf((*template.JS)(nil)).Elem()
	decs["JSEscape"] = template.JSEscape
	decs["JSEscapeString"] = template.JSEscapeString
	decs["JSEscaper"] = template.JSEscaper
	decs["JSStr"] = reflect.TypeOf((*template.JSStr)(nil)).Elem()
	decs["Must"] = template.Must
	decs["New"] = template.New
	decs["OK"] = template.OK
	decs["ParseFS"] = template.ParseFS
	decs["ParseFiles"] = template.ParseFiles
	decs["ParseGlob"] = template.ParseGlob
	decs["Srcset"] = reflect.TypeOf((*template.Srcset)(nil)).Elem()
	decs["Template"] = reflect.TypeOf((*template.Template)(nil)).Elem()
	decs["URL"] = reflect.TypeOf((*template.URL)(nil)).Elem()
	decs["URLQueryEscaper"] = template.URLQueryEscaper
	packages["html/template"] = native.Package{
		Name:         "template",
		Declarations: decs,
	}
	// "image"
	decs = make(native.Declarations, 50)
	decs["Alpha"] = reflect.TypeOf((*image.Alpha)(nil)).Elem()
	decs["Alpha16"] = reflect.TypeOf((*image.Alpha16)(nil)).Elem()
	decs["Black"] = &image.Black
	decs["CMYK"] = reflect.TypeOf((*image.CMYK)(nil)).Elem()
	decs["Config"] = reflect.TypeOf((*image.Config)(nil)).Elem()
	decs["Decode"] = image.Decode
	decs["DecodeConfig"] = image.DecodeConfig
	decs["ErrFormat"] = &image.ErrFormat
	decs["Gray"] = reflect.TypeOf((*image.Gray)(nil)).Elem()
	decs["Gray16"] = reflect.TypeOf((*image.Gray16)(nil)).Elem()
	decs["Image"] = reflect.TypeOf((*image.Image)(nil)).Elem()
	decs["NRGBA"] = reflect.TypeOf((*image.NRGBA)(nil)).Elem()
	decs["NRGBA64"] = reflect.TypeOf((*image.NRGBA64)(nil)).Elem()
	decs["NYCbCrA"] = reflect.TypeOf((*image.NYCbCrA)(nil)).Elem()
	decs["NewAlpha"] = image.NewAlpha
	decs["NewAlpha16"] = image.NewAlpha16
	decs["NewCMYK"] = image.NewCMYK
	decs["NewGray"] = image.NewGray
	decs["NewGray16"] = image.NewGray16
	decs["NewNRGBA"] = image.NewNRGBA
	decs["NewNRGBA64"] = image.NewNRGBA64
	decs["NewNYCbCrA"] = image.NewNYCbCrA
	decs["NewPaletted"] = image.NewPaletted
	decs["NewRGBA"] = image.NewRGBA
	decs["NewRGBA64"] = image.NewRGBA64
	decs["NewUniform"] = image.NewUniform
	decs["NewYCbCr"] = image.NewYCbCr
	decs["Opaque"] = &image.Opaque
	decs["Paletted"] = reflect.TypeOf((*image.Paletted)(nil)).Elem()
	decs["PalettedImage"] = reflect.TypeOf((*image.PalettedImage)(nil)).Elem()
	decs["Point"] = reflect.TypeOf((*image.Point)(nil)).Elem()
	decs["Pt"] = image.Pt
	decs["RGBA"] = reflect.TypeOf((*image.RGBA)(nil)).Elem()
	decs["RGBA64"] = reflect.TypeOf((*image.RGBA64)(nil)).Elem()
	decs["Rect"] = image.Rect
	decs["Rectangle"] = reflect.TypeOf((*image.Rectangle)(nil)).Elem()
	decs["RegisterFormat"] = image.RegisterFormat
	decs["Transparent"] = &image.Transparent
	decs["Uniform"] = reflect.TypeOf((*image.Uniform)(nil)).Elem()
	decs["White"] = &image.White
	decs["YCbCr"] = reflect.TypeOf((*image.YCbCr)(nil)).Elem()
	decs["YCbCrSubsampleRatio"] = reflect.TypeOf((*image.YCbCrSubsampleRatio)(nil)).Elem()
	decs["YCbCrSubsampleRatio410"] = image.YCbCrSubsampleRatio410
	decs["YCbCrSubsampleRatio411"] = image.YCbCrSubsampleRatio411
	decs["YCbCrSubsampleRatio420"] = image.YCbCrSubsampleRatio420
	decs["YCbCrSubsampleRatio422"] = image.YCbCrSubsampleRatio422
	decs["YCbCrSubsampleRatio440"] = image.YCbCrSubsampleRatio440
	decs["YCbCrSubsampleRatio444"] = image.YCbCrSubsampleRatio444
	decs["ZP"] = &image.ZP
	decs["ZR"] = &image.ZR
	packages["image"] = native.Package{
		Name:         "image",
		Declarations: decs,
	}
	// "image/color"
	decs = make(native.Declarations, 34)
	decs["Alpha"] = reflect.TypeOf((*color.Alpha)(nil)).Elem()
	decs["Alpha16"] = reflect.TypeOf((*color.Alpha16)(nil)).Elem()
	decs["Alpha16Model"] = &color.Alpha16Model
	decs["AlphaModel"] = &color.AlphaModel
	decs["Black"] = &color.Black
	decs["CMYK"] = reflect.TypeOf((*color.CMYK)(nil)).Elem()
	decs["CMYKModel"] = &color.CMYKModel
	decs["CMYKToRGB"] = color.CMYKToRGB
	decs["Color"] = reflect.TypeOf((*color.Color)(nil)).Elem()
	decs["Gray"] = reflect.TypeOf((*color.Gray)(nil)).Elem()
	decs["Gray16"] = reflect.TypeOf((*color.Gray16)(nil)).Elem()
	decs["Gray16Model"] = &color.Gray16Model
	decs["GrayModel"] = &color.GrayModel
	decs["Model"] = reflect.TypeOf((*color.Model)(nil)).Elem()
	decs["ModelFunc"] = color.ModelFunc
	decs["NRGBA"] = reflect.TypeOf((*color.NRGBA)(nil)).Elem()
	decs["NRGBA64"] = reflect.TypeOf((*color.NRGBA64)(nil)).Elem()
	decs["NRGBA64Model"] = &color.NRGBA64Model
	decs["NRGBAModel"] = &color.NRGBAModel
	decs["NYCbCrA"] = reflect.TypeOf((*color.NYCbCrA)(nil)).Elem()
	decs["NYCbCrAModel"] = &color.NYCbCrAModel
	decs["Opaque"] = &color.Opaque
	decs["Palette"] = reflect.TypeOf((*color.Palette)(nil)).Elem()
	decs["RGBA"] = reflect.TypeOf((*color.RGBA)(nil)).Elem()
	decs["RGBA64"] = reflect.TypeOf((*color.RGBA64)(nil)).Elem()
	decs["RGBA64Model"] = &color.RGBA64Model
	decs["RGBAModel"] = &color.RGBAModel
	decs["RGBToCMYK"] = color.RGBToCMYK
	decs["RGBToYCbCr"] = color.RGBToYCbCr
	decs["Transparent"] = &color.Transparent
	decs["White"] = &color.White
	decs["YCbCr"] = reflect.TypeOf((*color.YCbCr)(nil)).Elem()
	decs["YCbCrModel"] = &color.YCbCrModel
	decs["YCbCrToRGB"] = color.YCbCrToRGB
	packages["image/color"] = native.Package{
		Name:         "color",
		Declarations: decs,
	}
	// "image/color/palette"
	decs = make(native.Declarations, 2)
	decs["Plan9"] = &palette.Plan9
	decs["WebSafe"] = &palette.WebSafe
	packages["image/color/palette"] = native.Package{
		Name:         "palette",
		Declarations: decs,
	}
	// "image/draw"
	decs = make(native.Declarations, 9)
	decs["Draw"] = draw.Draw
	decs["DrawMask"] = draw.DrawMask
	decs["Drawer"] = reflect.TypeOf((*draw.Drawer)(nil)).Elem()
	decs["FloydSteinberg"] = &draw.FloydSteinberg
	decs["Image"] = reflect.TypeOf((*draw.Image)(nil)).Elem()
	decs["Op"] = reflect.TypeOf((*draw.Op)(nil)).Elem()
	decs["Over"] = draw.Over
	decs["Quantizer"] = reflect.TypeOf((*draw.Quantizer)(nil)).Elem()
	decs["Src"] = draw.Src
	packages["image/draw"] = native.Package{
		Name:         "draw",
		Declarations: decs,
	}
	// "image/gif"
	decs = make(native.Declarations, 10)
	decs["Decode"] = gif.Decode
	decs["DecodeAll"] = gif.DecodeAll
	decs["DecodeConfig"] = gif.DecodeConfig
	decs["DisposalBackground"] = native.UntypedNumericConst("2")
	decs["DisposalNone"] = native.UntypedNumericConst("1")
	decs["DisposalPrevious"] = native.UntypedNumericConst("3")
	decs["Encode"] = gif.Encode
	decs["EncodeAll"] = gif.EncodeAll
	decs["GIF"] = reflect.TypeOf((*gif.GIF)(nil)).Elem()
	decs["Options"] = reflect.TypeOf((*gif.Options)(nil)).Elem()
	packages["image/gif"] = native.Package{
		Name:         "gif",
		Declarations: decs,
	}
	// "image/jpeg"
	decs = make(native.Declarations, 8)
	decs["Decode"] = jpeg.Decode
	decs["DecodeConfig"] = jpeg.DecodeConfig
	decs["DefaultQuality"] = native.UntypedNumericConst("75")
	decs["Encode"] = jpeg.Encode
	decs["FormatError"] = reflect.TypeOf((*jpeg.FormatError)(nil)).Elem()
	decs["Options"] = reflect.TypeOf((*jpeg.Options)(nil)).Elem()
	decs["Reader"] = reflect.TypeOf((*jpeg.Reader)(nil)).Elem()
	decs["UnsupportedError"] = reflect.TypeOf((*jpeg.UnsupportedError)(nil)).Elem()
	packages["image/jpeg"] = native.Package{
		Name:         "jpeg",
		Declarations: decs,
	}
	// "image/png"
	decs = make(native.Declarations, 13)
	decs["BestCompression"] = png.BestCompression
	decs["BestSpeed"] = png.BestSpeed
	decs["CompressionLevel"] = reflect.TypeOf((*png.CompressionLevel)(nil)).Elem()
	decs["Decode"] = png.Decode
	decs["DecodeConfig"] = png.DecodeConfig
	decs["DefaultCompression"] = png.DefaultCompression
	decs["Encode"] = png.Encode
	decs["Encoder"] = reflect.TypeOf((*png.Encoder)(nil)).Elem()
	decs["EncoderBuffer"] = reflect.TypeOf((*png.EncoderBuffer)(nil)).Elem()
	decs["EncoderBufferPool"] = reflect.TypeOf((*png.EncoderBufferPool)(nil)).Elem()
	decs["FormatError"] = reflect.TypeOf((*png.FormatError)(nil)).Elem()
	decs["NoCompression"] = png.NoCompression
	decs["UnsupportedError"] = reflect.TypeOf((*png.UnsupportedError)(nil)).Elem()
	packages["image/png"] = native.Package{
		Name:         "png",
		Declarations: decs,
	}
	// "index/suffixarray"
	decs = make(native.Declarations, 2)
	decs["Index"] = reflect.TypeOf((*suffixarray.Index)(nil)).Elem()
	decs["New"] = suffixarray.New
	packages["index/suffixarray"] = native.Package{
		Name:         "suffixarray",
		Declarations: decs,
	}
	// "io"
	decs = make(native.Declarations, 50)
	decs["ByteReader"] = reflect.TypeOf((*io.ByteReader)(nil)).Elem()
	decs["ByteScanner"] = reflect.TypeOf((*io.ByteScanner)(nil)).Elem()
	decs["ByteWriter"] = reflect.TypeOf((*io.ByteWriter)(nil)).Elem()
	decs["Closer"] = reflect.TypeOf((*io.Closer)(nil)).Elem()
	decs["Copy"] = io.Copy
	decs["CopyBuffer"] = io.CopyBuffer
	decs["CopyN"] = io.CopyN
	decs["Discard"] = &io.Discard
	decs["EOF"] = &io.EOF
	decs["ErrClosedPipe"] = &io.ErrClosedPipe
	decs["ErrNoProgress"] = &io.ErrNoProgress
	decs["ErrShortBuffer"] = &io.ErrShortBuffer
	decs["ErrShortWrite"] = &io.ErrShortWrite
	decs["ErrUnexpectedEOF"] = &io.ErrUnexpectedEOF
	decs["LimitReader"] = io.LimitReader
	decs["LimitedReader"] = reflect.TypeOf((*io.LimitedReader)(nil)).Elem()
	decs["MultiReader"] = io.MultiReader
	decs["MultiWriter"] = io.MultiWriter
	decs["NewSectionReader"] = io.NewSectionReader
	decs["NopCloser"] = io.NopCloser
	decs["Pipe"] = io.Pipe
	decs["PipeReader"] = reflect.TypeOf((*io.PipeReader)(nil)).Elem()
	decs["PipeWriter"] = reflect.TypeOf((*io.PipeWriter)(nil)).Elem()
	decs["ReadAll"] = io.ReadAll
	decs["ReadAtLeast"] = io.ReadAtLeast
	decs["ReadCloser"] = reflect.TypeOf((*io.ReadCloser)(nil)).Elem()
	decs["ReadFull"] = io.ReadFull
	decs["ReadSeekCloser"] = reflect.TypeOf((*io.ReadSeekCloser)(nil)).Elem()
	decs["ReadSeeker"] = reflect.TypeOf((*io.ReadSeeker)(nil)).Elem()
	decs["ReadWriteCloser"] = reflect.TypeOf((*io.ReadWriteCloser)(nil)).Elem()
	decs["ReadWriteSeeker"] = reflect.TypeOf((*io.ReadWriteSeeker)(nil)).Elem()
	decs["ReadWriter"] = reflect.TypeOf((*io.ReadWriter)(nil)).Elem()
	decs["Reader"] = reflect.TypeOf((*io.Reader)(nil)).Elem()
	decs["ReaderAt"] = reflect.TypeOf((*io.ReaderAt)(nil)).Elem()
	decs["ReaderFrom"] = reflect.TypeOf((*io.ReaderFrom)(nil)).Elem()
	decs["RuneReader"] = reflect.TypeOf((*io.RuneReader)(nil)).Elem()
	decs["RuneScanner"] = reflect.TypeOf((*io.RuneScanner)(nil)).Elem()
	decs["SectionReader"] = reflect.TypeOf((*io.SectionReader)(nil)).Elem()
	decs["SeekCurrent"] = native.UntypedNumericConst("1")
	decs["SeekEnd"] = native.UntypedNumericConst("2")
	decs["SeekStart"] = native.UntypedNumericConst("0")
	decs["Seeker"] = reflect.TypeOf((*io.Seeker)(nil)).Elem()
	decs["StringWriter"] = reflect.TypeOf((*io.StringWriter)(nil)).Elem()
	decs["TeeReader"] = io.TeeReader
	decs["WriteCloser"] = reflect.TypeOf((*io.WriteCloser)(nil)).Elem()
	decs["WriteSeeker"] = reflect.TypeOf((*io.WriteSeeker)(nil)).Elem()
	decs["WriteString"] = io.WriteString
	decs["Writer"] = reflect.TypeOf((*io.Writer)(nil)).Elem()
	decs["WriterAt"] = reflect.TypeOf((*io.WriterAt)(nil)).Elem()
	decs["WriterTo"] = reflect.TypeOf((*io.WriterTo)(nil)).Elem()
	packages["io"] = native.Package{
		Name:         "io",
		Declarations: decs,
	}
	// "io/fs"
	decs = make(native.Declarations, 41)
	decs["DirEntry"] = reflect.TypeOf((*fs.DirEntry)(nil)).Elem()
	decs["ErrClosed"] = &fs.ErrClosed
	decs["ErrExist"] = &fs.ErrExist
	decs["ErrInvalid"] = &fs.ErrInvalid
	decs["ErrNotExist"] = &fs.ErrNotExist
	decs["ErrPermission"] = &fs.ErrPermission
	decs["FS"] = reflect.TypeOf((*fs.FS)(nil)).Elem()
	decs["File"] = reflect.TypeOf((*fs.File)(nil)).Elem()
	decs["FileInfo"] = reflect.TypeOf((*fs.FileInfo)(nil)).Elem()
	decs["FileMode"] = reflect.TypeOf((*fs.FileMode)(nil)).Elem()
	decs["Glob"] = fs.Glob
	decs["GlobFS"] = reflect.TypeOf((*fs.GlobFS)(nil)).Elem()
	decs["ModeAppend"] = fs.ModeAppend
	decs["ModeCharDevice"] = fs.ModeCharDevice
	decs["ModeDevice"] = fs.ModeDevice
	decs["ModeDir"] = fs.ModeDir
	decs["ModeExclusive"] = fs.ModeExclusive
	decs["ModeIrregular"] = fs.ModeIrregular
	decs["ModeNamedPipe"] = fs.ModeNamedPipe
	decs["ModePerm"] = fs.ModePerm
	decs["ModeSetgid"] = fs.ModeSetgid
	decs["ModeSetuid"] = fs.ModeSetuid
	decs["ModeSocket"] = fs.ModeSocket
	decs["ModeSticky"] = fs.ModeSticky
	decs["ModeSymlink"] = fs.ModeSymlink
	decs["ModeTemporary"] = fs.ModeTemporary
	decs["ModeType"] = fs.ModeType
	decs["PathError"] = reflect.TypeOf((*fs.PathError)(nil)).Elem()
	decs["ReadDir"] = fs.ReadDir
	decs["ReadDirFS"] = reflect.TypeOf((*fs.ReadDirFS)(nil)).Elem()
	decs["ReadDirFile"] = reflect.TypeOf((*fs.ReadDirFile)(nil)).Elem()
	decs["ReadFile"] = fs.ReadFile
	decs["ReadFileFS"] = reflect.TypeOf((*fs.ReadFileFS)(nil)).Elem()
	decs["SkipDir"] = &fs.SkipDir
	decs["Stat"] = fs.Stat
	decs["StatFS"] = reflect.TypeOf((*fs.StatFS)(nil)).Elem()
	decs["Sub"] = fs.Sub
	decs["SubFS"] = reflect.TypeOf((*fs.SubFS)(nil)).Elem()
	decs["ValidPath"] = fs.ValidPath
	decs["WalkDir"] = fs.WalkDir
	decs["WalkDirFunc"] = reflect.TypeOf((*fs.WalkDirFunc)(nil)).Elem()
	packages["io/fs"] = native.Package{
		Name:         "fs",
		Declarations: decs,
	}
	// "io/ioutil"
	decs = make(native.Declarations, 8)
	decs["Discard"] = &ioutil.Discard
	decs["NopCloser"] = ioutil.NopCloser
	decs["ReadAll"] = ioutil.ReadAll
	decs["ReadDir"] = ioutil.ReadDir
	decs["ReadFile"] = ioutil.ReadFile
	decs["TempDir"] = ioutil.TempDir
	decs["TempFile"] = ioutil.TempFile
	decs["WriteFile"] = ioutil.WriteFile
	packages["io/ioutil"] = native.Package{
		Name:         "ioutil",
		Declarations: decs,
	}
	// "log"
	decs = make(native.Declarations, 27)
	decs["Default"] = log.Default
	decs["Fatal"] = log.Fatal
	decs["Fatalf"] = log.Fatalf
	decs["Fatalln"] = log.Fatalln
	decs["Flags"] = log.Flags
	decs["LUTC"] = native.UntypedNumericConst("32")
	decs["Ldate"] = native.UntypedNumericConst("1")
	decs["Llongfile"] = native.UntypedNumericConst("8")
	decs["Lmicroseconds"] = native.UntypedNumericConst("4")
	decs["Lmsgprefix"] = native.UntypedNumericConst("64")
	decs["Logger"] = reflect.TypeOf((*log.Logger)(nil)).Elem()
	decs["Lshortfile"] = native.UntypedNumericConst("16")
	decs["LstdFlags"] = native.UntypedNumericConst("3")
	decs["Ltime"] = native.UntypedNumericConst("2")
	decs["New"] = log.New
	decs["Output"] = log.Output
	decs["Panic"] = log.Panic
	decs["Panicf"] = log.Panicf
	decs["Panicln"] = log.Panicln
	decs["Prefix"] = log.Prefix
	decs["Print"] = log.Print
	decs["Printf"] = log.Printf
	decs["Println"] = log.Println
	decs["SetFlags"] = log.SetFlags
	decs["SetOutput"] = log.SetOutput
	decs["SetPrefix"] = log.SetPrefix
	decs["Writer"] = log.Writer
	packages["log"] = native.Package{
		Name:         "log",
		Declarations: decs,
	}
	// "log/syslog"
	decs = make(native.Declarations, 33)
	decs["Dial"] = syslog.Dial
	decs["LOG_ALERT"] = syslog.LOG_ALERT
	decs["LOG_AUTH"] = syslog.LOG_AUTH
	decs["LOG_AUTHPRIV"] = syslog.LOG_AUTHPRIV
	decs["LOG_CRIT"] = syslog.LOG_CRIT
	decs["LOG_CRON"] = syslog.LOG_CRON
	decs["LOG_DAEMON"] = syslog.LOG_DAEMON
	decs["LOG_DEBUG"] = syslog.LOG_DEBUG
	decs["LOG_EMERG"] = syslog.LOG_EMERG
	decs["LOG_ERR"] = syslog.LOG_ERR
	decs["LOG_FTP"] = syslog.LOG_FTP
	decs["LOG_INFO"] = syslog.LOG_INFO
	decs["LOG_KERN"] = syslog.LOG_KERN
	decs["LOG_LOCAL0"] = syslog.LOG_LOCAL0
	decs["LOG_LOCAL1"] = syslog.LOG_LOCAL1
	decs["LOG_LOCAL2"] = syslog.LOG_LOCAL2
	decs["LOG_LOCAL3"] = syslog.LOG_LOCAL3
	decs["LOG_LOCAL4"] = syslog.LOG_LOCAL4
	decs["LOG_LOCAL5"] = syslog.LOG_LOCAL5
	decs["LOG_LOCAL6"] = syslog.LOG_LOCAL6
	decs["LOG_LOCAL7"] = syslog.LOG_LOCAL7
	decs["LOG_LPR"] = syslog.LOG_LPR
	decs["LOG_MAIL"] = syslog.LOG_MAIL
	decs["LOG_NEWS"] = syslog.LOG_NEWS
	decs["LOG_NOTICE"] = syslog.LOG_NOTICE
	decs["LOG_SYSLOG"] = syslog.LOG_SYSLOG
	decs["LOG_USER"] = syslog.LOG_USER
	decs["LOG_UUCP"] = syslog.LOG_UUCP
	decs["LOG_WARNING"] = syslog.LOG_WARNING
	decs["New"] = syslog.New
	decs["NewLogger"] = syslog.NewLogger
	decs["Priority"] = reflect.TypeOf((*syslog.Priority)(nil)).Elem()
	decs["Writer"] = reflect.TypeOf((*syslog.Writer)(nil)).Elem()
	packages["log/syslog"] = native.Package{
		Name:         "syslog",
		Declarations: decs,
	}
	// "math"
	decs = make(native.Declarations, 94)
	decs["Abs"] = math.Abs
	decs["Acos"] = math.Acos
	decs["Acosh"] = math.Acosh
	decs["Asin"] = math.Asin
	decs["Asinh"] = math.Asinh
	decs["Atan"] = math.Atan
	decs["Atan2"] = math.Atan2
	decs["Atanh"] = math.Atanh
	decs["Cbrt"] = math.Cbrt
	decs["Ceil"] = math.Ceil
	decs["Copysign"] = math.Copysign
	decs["Cos"] = math.Cos
	decs["Cosh"] = math.Cosh
	decs["Dim"] = math.Dim
	decs["E"] = native.UntypedNumericConst("2.71828182845904523536028747135266249775724709369995957496696763")
	decs["Erf"] = math.Erf
	decs["Erfc"] = math.Erfc
	decs["Erfcinv"] = math.Erfcinv
	decs["Erfinv"] = math.Erfinv
	decs["Exp"] = math.Exp
	decs["Exp2"] = math.Exp2
	decs["Expm1"] = math.Expm1
	decs["FMA"] = math.FMA
	decs["Float32bits"] = math.Float32bits
	decs["Float32frombits"] = math.Float32frombits
	decs["Float64bits"] = math.Float64bits
	decs["Float64frombits"] = math.Float64frombits
	decs["Floor"] = math.Floor
	decs["Frexp"] = math.Frexp
	decs["Gamma"] = math.Gamma
	decs["Hypot"] = math.Hypot
	decs["Ilogb"] = math.Ilogb
	decs["Inf"] = math.Inf
	decs["IsInf"] = math.IsInf
	decs["IsNaN"] = math.IsNaN
	decs["J0"] = math.J0
	decs["J1"] = math.J1
	decs["Jn"] = math.Jn
	decs["Ldexp"] = math.Ldexp
	decs["Lgamma"] = math.Lgamma
	decs["Ln10"] = native.UntypedNumericConst("2.3025850929940456840179914546843642076011014886287729760333279")
	decs["Ln2"] = native.UntypedNumericConst("0.693147180559945309417232121458176568075500134360255254120680009")
	decs["Log"] = math.Log
	decs["Log10"] = math.Log10
	decs["Log10E"] = native.UntypedNumericConst("10000000000000000000000000000000000000000000000000000000000000/23025850929940456840179914546843642076011014886287729760333279")
	decs["Log1p"] = math.Log1p
	decs["Log2"] = math.Log2
	decs["Log2E"] = native.UntypedNumericConst("1000000000000000000000000000000000000000000000000000000000000000/693147180559945309417232121458176568075500134360255254120680009")
	decs["Logb"] = math.Logb
	decs["Max"] = math.Max
	decs["MaxFloat32"] = native.UntypedNumericConst("340282346638528859811704183484516925440.0")
	decs["MaxFloat64"] = native.UntypedNumericConst("179769313486231570814527423731704356798100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.0")
	decs["MaxInt16"] = native.UntypedNumericConst("32767")
	decs["MaxInt32"] = native.UntypedNumericConst("2147483647")
	decs["MaxInt64"] = native.UntypedNumericConst("9223372036854775807")
	decs["MaxInt8"] = native.UntypedNumericConst("127")
	decs["MaxUint16"] = native.UntypedNumericConst("65535")
	decs["MaxUint32"] = native.UntypedNumericConst("4294967295")
	decs["MaxUint64"] = native.UntypedNumericConst("18446744073709551615")
	decs["MaxUint8"] = native.UntypedNumericConst("255")
	decs["Min"] = math.Min
	decs["MinInt16"] = native.UntypedNumericConst("-32768")
	decs["MinInt32"] = native.UntypedNumericConst("-2147483648")
	decs["MinInt64"] = native.UntypedNumericConst("-9223372036854775808")
	decs["MinInt8"] = native.UntypedNumericConst("-128")
	decs["Mod"] = math.Mod
	decs["Modf"] = math.Modf
	decs["NaN"] = math.NaN
	decs["Nextafter"] = math.Nextafter
	decs["Nextafter32"] = math.Nextafter32
	decs["Phi"] = native.UntypedNumericConst("1.61803398874989484820458683436563811772030917980576286213544862")
	decs["Pi"] = native.UntypedNumericConst("3.14159265358979323846264338327950288419716939937510582097494459")
	decs["Pow"] = math.Pow
	decs["Pow10"] = math.Pow10
	decs["Remainder"] = math.Remainder
	decs["Round"] = math.Round
	decs["RoundToEven"] = math.RoundToEven
	decs["Signbit"] = math.Signbit
	decs["Sin"] = math.Sin
	decs["Sincos"] = math.Sincos
	decs["Sinh"] = math.Sinh
	decs["SmallestNonzeroFloat32"] = native.UntypedNumericConst("1.40129846432481707092372958328991613128e-45")
	decs["SmallestNonzeroFloat64"] = native.UntypedNumericConst("4.940656458412465441765687928682213723651e-324")
	decs["Sqrt"] = math.Sqrt
	decs["Sqrt2"] = native.UntypedNumericConst("1.41421356237309504880168872420969807856967187537694807317667974")
	decs["SqrtE"] = native.UntypedNumericConst("1.64872127070012814684865078781416357165377610071014801157507931")
	decs["SqrtPhi"] = native.UntypedNumericConst("1.27201964951406896425242246173749149171560804184009624861664038")
	decs["SqrtPi"] = native.UntypedNumericConst("1.77245385090551602729816748334114518279754945612238712821380779")
	decs["Tan"] = math.Tan
	decs["Tanh"] = math.Tanh
	decs["Trunc"] = math.Trunc
	decs["Y0"] = math.Y0
	decs["Y1"] = math.Y1
	decs["Yn"] = math.Yn
	packages["math"] = native.Package{
		Name:         "math",
		Declarations: decs,
	}
	// "math/big"
	decs = make(native.Declarations, 25)
	decs["Above"] = big.Above
	decs["Accuracy"] = reflect.TypeOf((*big.Accuracy)(nil)).Elem()
	decs["AwayFromZero"] = big.AwayFromZero
	decs["Below"] = big.Below
	decs["ErrNaN"] = reflect.TypeOf((*big.ErrNaN)(nil)).Elem()
	decs["Exact"] = big.Exact
	decs["Float"] = reflect.TypeOf((*big.Float)(nil)).Elem()
	decs["Int"] = reflect.TypeOf((*big.Int)(nil)).Elem()
	decs["Jacobi"] = big.Jacobi
	decs["MaxBase"] = native.UntypedNumericConst("62")
	decs["MaxExp"] = native.UntypedNumericConst("2147483647")
	decs["MaxPrec"] = native.UntypedNumericConst("4294967295")
	decs["MinExp"] = native.UntypedNumericConst("-2147483648")
	decs["NewFloat"] = big.NewFloat
	decs["NewInt"] = big.NewInt
	decs["NewRat"] = big.NewRat
	decs["ParseFloat"] = big.ParseFloat
	decs["Rat"] = reflect.TypeOf((*big.Rat)(nil)).Elem()
	decs["RoundingMode"] = reflect.TypeOf((*big.RoundingMode)(nil)).Elem()
	decs["ToNearestAway"] = big.ToNearestAway
	decs["ToNearestEven"] = big.ToNearestEven
	decs["ToNegativeInf"] = big.ToNegativeInf
	decs["ToPositiveInf"] = big.ToPositiveInf
	decs["ToZero"] = big.ToZero
	decs["Word"] = reflect.TypeOf((*big.Word)(nil)).Elem()
	packages["math/big"] = native.Package{
		Name:         "big",
		Declarations: decs,
	}
	// "math/bits"
	decs = make(native.Declarations, 50)
	decs["Add"] = bits.Add
	decs["Add32"] = bits.Add32
	decs["Add64"] = bits.Add64
	decs["Div"] = bits.Div
	decs["Div32"] = bits.Div32
	decs["Div64"] = bits.Div64
	decs["LeadingZeros"] = bits.LeadingZeros
	decs["LeadingZeros16"] = bits.LeadingZeros16
	decs["LeadingZeros32"] = bits.LeadingZeros32
	decs["LeadingZeros64"] = bits.LeadingZeros64
	decs["LeadingZeros8"] = bits.LeadingZeros8
	decs["Len"] = bits.Len
	decs["Len16"] = bits.Len16
	decs["Len32"] = bits.Len32
	decs["Len64"] = bits.Len64
	decs["Len8"] = bits.Len8
	decs["Mul"] = bits.Mul
	decs["Mul32"] = bits.Mul32
	decs["Mul64"] = bits.Mul64
	decs["OnesCount"] = bits.OnesCount
	decs["OnesCount16"] = bits.OnesCount16
	decs["OnesCount32"] = bits.OnesCount32
	decs["OnesCount64"] = bits.OnesCount64
	decs["OnesCount8"] = bits.OnesCount8
	decs["Rem"] = bits.Rem
	decs["Rem32"] = bits.Rem32
	decs["Rem64"] = bits.Rem64
	decs["Reverse"] = bits.Reverse
	decs["Reverse16"] = bits.Reverse16
	decs["Reverse32"] = bits.Reverse32
	decs["Reverse64"] = bits.Reverse64
	decs["Reverse8"] = bits.Reverse8
	decs["ReverseBytes"] = bits.ReverseBytes
	decs["ReverseBytes16"] = bits.ReverseBytes16
	decs["ReverseBytes32"] = bits.ReverseBytes32
	decs["ReverseBytes64"] = bits.ReverseBytes64
	decs["RotateLeft"] = bits.RotateLeft
	decs["RotateLeft16"] = bits.RotateLeft16
	decs["RotateLeft32"] = bits.RotateLeft32
	decs["RotateLeft64"] = bits.RotateLeft64
	decs["RotateLeft8"] = bits.RotateLeft8
	decs["Sub"] = bits.Sub
	decs["Sub32"] = bits.Sub32
	decs["Sub64"] = bits.Sub64
	decs["TrailingZeros"] = bits.TrailingZeros
	decs["TrailingZeros16"] = bits.TrailingZeros16
	decs["TrailingZeros32"] = bits.TrailingZeros32
	decs["TrailingZeros64"] = bits.TrailingZeros64
	decs["TrailingZeros8"] = bits.TrailingZeros8
	decs["UintSize"] = native.UntypedNumericConst("64")
	packages["math/bits"] = native.Package{
		Name:         "bits",
		Declarations: decs,
	}
	// "math/cmplx"
	decs = make(native.Declarations, 27)
	decs["Abs"] = cmplx.Abs
	decs["Acos"] = cmplx.Acos
	decs["Acosh"] = cmplx.Acosh
	decs["Asin"] = cmplx.Asin
	decs["Asinh"] = cmplx.Asinh
	decs["Atan"] = cmplx.Atan
	decs["Atanh"] = cmplx.Atanh
	decs["Conj"] = cmplx.Conj
	decs["Cos"] = cmplx.Cos
	decs["Cosh"] = cmplx.Cosh
	decs["Cot"] = cmplx.Cot
	decs["Exp"] = cmplx.Exp
	decs["Inf"] = cmplx.Inf
	decs["IsInf"] = cmplx.IsInf
	decs["IsNaN"] = cmplx.IsNaN
	decs["Log"] = cmplx.Log
	decs["Log10"] = cmplx.Log10
	decs["NaN"] = cmplx.NaN
	decs["Phase"] = cmplx.Phase
	decs["Polar"] = cmplx.Polar
	decs["Pow"] = cmplx.Pow
	decs["Rect"] = cmplx.Rect
	decs["Sin"] = cmplx.Sin
	decs["Sinh"] = cmplx.Sinh
	decs["Sqrt"] = cmplx.Sqrt
	decs["Tan"] = cmplx.Tan
	decs["Tanh"] = cmplx.Tanh
	packages["math/cmplx"] = native.Package{
		Name:         "cmplx",
		Declarations: decs,
	}
	// "math/rand"
	decs = make(native.Declarations, 23)
	decs["ExpFloat64"] = rand_.ExpFloat64
	decs["Float32"] = rand_.Float32
	decs["Float64"] = rand_.Float64
	decs["Int"] = rand_.Int
	decs["Int31"] = rand_.Int31
	decs["Int31n"] = rand_.Int31n
	decs["Int63"] = rand_.Int63
	decs["Int63n"] = rand_.Int63n
	decs["Intn"] = rand_.Intn
	decs["New"] = rand_.New
	decs["NewSource"] = rand_.NewSource
	decs["NewZipf"] = rand_.NewZipf
	decs["NormFloat64"] = rand_.NormFloat64
	decs["Perm"] = rand_.Perm
	decs["Rand"] = reflect.TypeOf((*rand_.Rand)(nil)).Elem()
	decs["Read"] = rand_.Read
	decs["Seed"] = rand_.Seed
	decs["Shuffle"] = rand_.Shuffle
	decs["Source"] = reflect.TypeOf((*rand_.Source)(nil)).Elem()
	decs["Source64"] = reflect.TypeOf((*rand_.Source64)(nil)).Elem()
	decs["Uint32"] = rand_.Uint32
	decs["Uint64"] = rand_.Uint64
	decs["Zipf"] = reflect.TypeOf((*rand_.Zipf)(nil)).Elem()
	packages["math/rand"] = native.Package{
		Name:         "rand",
		Declarations: decs,
	}
	// "mime"
	decs = make(native.Declarations, 10)
	decs["AddExtensionType"] = mime.AddExtensionType
	decs["BEncoding"] = mime.BEncoding
	decs["ErrInvalidMediaParameter"] = &mime.ErrInvalidMediaParameter
	decs["ExtensionsByType"] = mime.ExtensionsByType
	decs["FormatMediaType"] = mime.FormatMediaType
	decs["ParseMediaType"] = mime.ParseMediaType
	decs["QEncoding"] = mime.QEncoding
	decs["TypeByExtension"] = mime.TypeByExtension
	decs["WordDecoder"] = reflect.TypeOf((*mime.WordDecoder)(nil)).Elem()
	decs["WordEncoder"] = reflect.TypeOf((*mime.WordEncoder)(nil)).Elem()
	packages["mime"] = native.Package{
		Name:         "mime",
		Declarations: decs,
	}
	// "mime/multipart"
	decs = make(native.Declarations, 9)
	decs["ErrMessageTooLarge"] = &multipart.ErrMessageTooLarge
	decs["File"] = reflect.TypeOf((*multipart.File)(nil)).Elem()
	decs["FileHeader"] = reflect.TypeOf((*multipart.FileHeader)(nil)).Elem()
	decs["Form"] = reflect.TypeOf((*multipart.Form)(nil)).Elem()
	decs["NewReader"] = multipart.NewReader
	decs["NewWriter"] = multipart.NewWriter
	decs["Part"] = reflect.TypeOf((*multipart.Part)(nil)).Elem()
	decs["Reader"] = reflect.TypeOf((*multipart.Reader)(nil)).Elem()
	decs["Writer"] = reflect.TypeOf((*multipart.Writer)(nil)).Elem()
	packages["mime/multipart"] = native.Package{
		Name:         "multipart",
		Declarations: decs,
	}
	// "mime/quotedprintable"
	decs = make(native.Declarations, 4)
	decs["NewReader"] = quotedprintable.NewReader
	decs["NewWriter"] = quotedprintable.NewWriter
	decs["Reader"] = reflect.TypeOf((*quotedprintable.Reader)(nil)).Elem()
	decs["Writer"] = reflect.TypeOf((*quotedprintable.Writer)(nil)).Elem()
	packages["mime/quotedprintable"] = native.Package{
		Name:         "quotedprintable",
		Declarations: decs,
	}
	// "net"
	decs = make(native.Declarations, 98)
	decs["Addr"] = reflect.TypeOf((*net.Addr)(nil)).Elem()
	decs["AddrError"] = reflect.TypeOf((*net.AddrError)(nil)).Elem()
	decs["Buffers"] = reflect.TypeOf((*net.Buffers)(nil)).Elem()
	decs["CIDRMask"] = net.CIDRMask
	decs["Conn"] = reflect.TypeOf((*net.Conn)(nil)).Elem()
	decs["DNSConfigError"] = reflect.TypeOf((*net.DNSConfigError)(nil)).Elem()
	decs["DNSError"] = reflect.TypeOf((*net.DNSError)(nil)).Elem()
	decs["DefaultResolver"] = &net.DefaultResolver
	decs["Dial"] = net.Dial
	decs["DialIP"] = net.DialIP
	decs["DialTCP"] = net.DialTCP
	decs["DialTimeout"] = net.DialTimeout
	decs["DialUDP"] = net.DialUDP
	decs["DialUnix"] = net.DialUnix
	decs["Dialer"] = reflect.TypeOf((*net.Dialer)(nil)).Elem()
	decs["ErrClosed"] = &net.ErrClosed
	decs["ErrWriteToConnected"] = &net.ErrWriteToConnected
	decs["Error"] = reflect.TypeOf((*net.Error)(nil)).Elem()
	decs["FileConn"] = net.FileConn
	decs["FileListener"] = net.FileListener
	decs["FilePacketConn"] = net.FilePacketConn
	decs["FlagBroadcast"] = net.FlagBroadcast
	decs["FlagLoopback"] = net.FlagLoopback
	decs["FlagMulticast"] = net.FlagMulticast
	decs["FlagPointToPoint"] = net.FlagPointToPoint
	decs["FlagUp"] = net.FlagUp
	decs["Flags"] = reflect.TypeOf((*net.Flags)(nil)).Elem()
	decs["HardwareAddr"] = reflect.TypeOf((*net.HardwareAddr)(nil)).Elem()
	decs["IP"] = reflect.TypeOf((*net.IP)(nil)).Elem()
	decs["IPAddr"] = reflect.TypeOf((*net.IPAddr)(nil)).Elem()
	decs["IPConn"] = reflect.TypeOf((*net.IPConn)(nil)).Elem()
	decs["IPMask"] = reflect.TypeOf((*net.IPMask)(nil)).Elem()
	decs["IPNet"] = reflect.TypeOf((*net.IPNet)(nil)).Elem()
	decs["IPv4"] = net.IPv4
	decs["IPv4Mask"] = net.IPv4Mask
	decs["IPv4allrouter"] = &net.IPv4allrouter
	decs["IPv4allsys"] = &net.IPv4allsys
	decs["IPv4bcast"] = &net.IPv4bcast
	decs["IPv4len"] = native.UntypedNumericConst("4")
	decs["IPv4zero"] = &net.IPv4zero
	decs["IPv6interfacelocalallnodes"] = &net.IPv6interfacelocalallnodes
	decs["IPv6len"] = native.UntypedNumericConst("16")
	decs["IPv6linklocalallnodes"] = &net.IPv6linklocalallnodes
	decs["IPv6linklocalallrouters"] = &net.IPv6linklocalallrouters
	decs["IPv6loopback"] = &net.IPv6loopback
	decs["IPv6unspecified"] = &net.IPv6unspecified
	decs["IPv6zero"] = &net.IPv6zero
	decs["Interface"] = reflect.TypeOf((*net.Interface)(nil)).Elem()
	decs["InterfaceAddrs"] = net.InterfaceAddrs
	decs["InterfaceByIndex"] = net.InterfaceByIndex
	decs["InterfaceByName"] = net.InterfaceByName
	decs["Interfaces"] = net.Interfaces
	decs["InvalidAddrError"] = reflect.TypeOf((*net.InvalidAddrError)(nil)).Elem()
	decs["JoinHostPort"] = net.JoinHostPort
	decs["Listen"] = net.Listen
	decs["ListenConfig"] = reflect.TypeOf((*net.ListenConfig)(nil)).Elem()
	decs["ListenIP"] = net.ListenIP
	decs["ListenMulticastUDP"] = net.ListenMulticastUDP
	decs["ListenPacket"] = net.ListenPacket
	decs["ListenTCP"] = net.ListenTCP
	decs["ListenUDP"] = net.ListenUDP
	decs["ListenUnix"] = net.ListenUnix
	decs["ListenUnixgram"] = net.ListenUnixgram
	decs["Listener"] = reflect.TypeOf((*net.Listener)(nil)).Elem()
	decs["LookupAddr"] = net.LookupAddr
	decs["LookupCNAME"] = net.LookupCNAME
	decs["LookupHost"] = net.LookupHost
	decs["LookupIP"] = net.LookupIP
	decs["LookupMX"] = net.LookupMX
	decs["LookupNS"] = net.LookupNS
	decs["LookupPort"] = net.LookupPort
	decs["LookupSRV"] = net.LookupSRV
	decs["LookupTXT"] = net.LookupTXT
	decs["MX"] = reflect.TypeOf((*net.MX)(nil)).Elem()
	decs["NS"] = reflect.TypeOf((*net.NS)(nil)).Elem()
	decs["OpError"] = reflect.TypeOf((*net.OpError)(nil)).Elem()
	decs["PacketConn"] = reflect.TypeOf((*net.PacketConn)(nil)).Elem()
	decs["ParseCIDR"] = net.ParseCIDR
	decs["ParseError"] = reflect.TypeOf((*net.ParseError)(nil)).Elem()
	decs["ParseIP"] = net.ParseIP
	decs["ParseMAC"] = net.ParseMAC
	decs["Pipe"] = net.Pipe
	decs["ResolveIPAddr"] = net.ResolveIPAddr
	decs["ResolveTCPAddr"] = net.ResolveTCPAddr
	decs["ResolveUDPAddr"] = net.ResolveUDPAddr
	decs["ResolveUnixAddr"] = net.ResolveUnixAddr
	decs["Resolver"] = reflect.TypeOf((*net.Resolver)(nil)).Elem()
	decs["SRV"] = reflect.TypeOf((*net.SRV)(nil)).Elem()
	decs["SplitHostPort"] = net.SplitHostPort
	decs["TCPAddr"] = reflect.TypeOf((*net.TCPAddr)(nil)).Elem()
	decs["TCPConn"] = reflect.TypeOf((*net.TCPConn)(nil)).Elem()
	decs["TCPListener"] = reflect.TypeOf((*net.TCPListener)(nil)).Elem()
	decs["UDPAddr"] = reflect.TypeOf((*net.UDPAddr)(nil)).Elem()
	decs["UDPConn"] = reflect.TypeOf((*net.UDPConn)(nil)).Elem()
	decs["UnixAddr"] = reflect.TypeOf((*net.UnixAddr)(nil)).Elem()
	decs["UnixConn"] = reflect.TypeOf((*net.UnixConn)(nil)).Elem()
	decs["UnixListener"] = reflect.TypeOf((*net.UnixListener)(nil)).Elem()
	decs["UnknownNetworkError"] = reflect.TypeOf((*net.UnknownNetworkError)(nil)).Elem()
	packages["net"] = native.Package{
		Name:         "net",
		Declarations: decs,
	}
	// "net/http"
	decs = make(native.Declarations, 171)
	decs["CanonicalHeaderKey"] = http.CanonicalHeaderKey
	decs["Client"] = reflect.TypeOf((*http.Client)(nil)).Elem()
	decs["CloseNotifier"] = reflect.TypeOf((*http.CloseNotifier)(nil)).Elem()
	decs["ConnState"] = reflect.TypeOf((*http.ConnState)(nil)).Elem()
	decs["Cookie"] = reflect.TypeOf((*http.Cookie)(nil)).Elem()
	decs["CookieJar"] = reflect.TypeOf((*http.CookieJar)(nil)).Elem()
	decs["DefaultClient"] = &http.DefaultClient
	decs["DefaultMaxHeaderBytes"] = native.UntypedNumericConst("1048576")
	decs["DefaultMaxIdleConnsPerHost"] = native.UntypedNumericConst("2")
	decs["DefaultServeMux"] = &http.DefaultServeMux
	decs["DefaultTransport"] = &http.DefaultTransport
	decs["DetectContentType"] = http.DetectContentType
	decs["Dir"] = reflect.TypeOf((*http.Dir)(nil)).Elem()
	decs["ErrAbortHandler"] = &http.ErrAbortHandler
	decs["ErrBodyNotAllowed"] = &http.ErrBodyNotAllowed
	decs["ErrBodyReadAfterClose"] = &http.ErrBodyReadAfterClose
	decs["ErrContentLength"] = &http.ErrContentLength
	decs["ErrHandlerTimeout"] = &http.ErrHandlerTimeout
	decs["ErrHeaderTooLong"] = &http.ErrHeaderTooLong
	decs["ErrHijacked"] = &http.ErrHijacked
	decs["ErrLineTooLong"] = &http.ErrLineTooLong
	decs["ErrMissingBoundary"] = &http.ErrMissingBoundary
	decs["ErrMissingContentLength"] = &http.ErrMissingContentLength
	decs["ErrMissingFile"] = &http.ErrMissingFile
	decs["ErrNoCookie"] = &http.ErrNoCookie
	decs["ErrNoLocation"] = &http.ErrNoLocation
	decs["ErrNotMultipart"] = &http.ErrNotMultipart
	decs["ErrNotSupported"] = &http.ErrNotSupported
	decs["ErrServerClosed"] = &http.ErrServerClosed
	decs["ErrShortBody"] = &http.ErrShortBody
	decs["ErrSkipAltProtocol"] = &http.ErrSkipAltProtocol
	decs["ErrUnexpectedTrailer"] = &http.ErrUnexpectedTrailer
	decs["ErrUseLastResponse"] = &http.ErrUseLastResponse
	decs["ErrWriteAfterFlush"] = &http.ErrWriteAfterFlush
	decs["Error"] = http.Error
	decs["FS"] = http.FS
	decs["File"] = reflect.TypeOf((*http.File)(nil)).Elem()
	decs["FileServer"] = http.FileServer
	decs["FileSystem"] = reflect.TypeOf((*http.FileSystem)(nil)).Elem()
	decs["Flusher"] = reflect.TypeOf((*http.Flusher)(nil)).Elem()
	decs["Get"] = http.Get
	decs["Handle"] = http.Handle
	decs["HandleFunc"] = http.HandleFunc
	decs["Handler"] = reflect.TypeOf((*http.Handler)(nil)).Elem()
	decs["HandlerFunc"] = reflect.TypeOf((*http.HandlerFunc)(nil)).Elem()
	decs["Head"] = http.Head
	decs["Header"] = reflect.TypeOf((*http.Header)(nil)).Elem()
	decs["Hijacker"] = reflect.TypeOf((*http.Hijacker)(nil)).Elem()
	decs["ListenAndServe"] = http.ListenAndServe
	decs["ListenAndServeTLS"] = http.ListenAndServeTLS
	decs["LocalAddrContextKey"] = &http.LocalAddrContextKey
	decs["MaxBytesReader"] = http.MaxBytesReader
	decs["MethodConnect"] = native.UntypedStringConst("CONNECT")
	decs["MethodDelete"] = native.UntypedStringConst("DELETE")
	decs["MethodGet"] = native.UntypedStringConst("GET")
	decs["MethodHead"] = native.UntypedStringConst("HEAD")
	decs["MethodOptions"] = native.UntypedStringConst("OPTIONS")
	decs["MethodPatch"] = native.UntypedStringConst("PATCH")
	decs["MethodPost"] = native.UntypedStringConst("POST")
	decs["MethodPut"] = native.UntypedStringConst("PUT")
	decs["MethodTrace"] = native.UntypedStringConst("TRACE")
	decs["NewFileTransport"] = http.NewFileTransport
	decs["NewRequest"] = http.NewRequest
	decs["NewRequestWithContext"] = http.NewRequestWithContext
	decs["NewServeMux"] = http.NewServeMux
	decs["NoBody"] = &http.NoBody
	decs["NotFound"] = http.NotFound
	decs["NotFoundHandler"] = http.NotFoundHandler
	decs["ParseHTTPVersion"] = http.ParseHTTPVersion
	decs["ParseTime"] = http.ParseTime
	decs["Post"] = http.Post
	decs["PostForm"] = http.PostForm
	decs["ProtocolError"] = reflect.TypeOf((*http.ProtocolError)(nil)).Elem()
	decs["ProxyFromEnvironment"] = http.ProxyFromEnvironment
	decs["ProxyURL"] = http.ProxyURL
	decs["PushOptions"] = reflect.TypeOf((*http.PushOptions)(nil)).Elem()
	decs["Pusher"] = reflect.TypeOf((*http.Pusher)(nil)).Elem()
	decs["ReadRequest"] = http.ReadRequest
	decs["ReadResponse"] = http.ReadResponse
	decs["Redirect"] = http.Redirect
	decs["RedirectHandler"] = http.RedirectHandler
	decs["Request"] = reflect.TypeOf((*http.Request)(nil)).Elem()
	decs["Response"] = reflect.TypeOf((*http.Response)(nil)).Elem()
	decs["ResponseWriter"] = reflect.TypeOf((*http.ResponseWriter)(nil)).Elem()
	decs["RoundTripper"] = reflect.TypeOf((*http.RoundTripper)(nil)).Elem()
	decs["SameSite"] = reflect.TypeOf((*http.SameSite)(nil)).Elem()
	decs["SameSiteDefaultMode"] = http.SameSiteDefaultMode
	decs["SameSiteLaxMode"] = http.SameSiteLaxMode
	decs["SameSiteNoneMode"] = http.SameSiteNoneMode
	decs["SameSiteStrictMode"] = http.SameSiteStrictMode
	decs["Serve"] = http.Serve
	decs["ServeContent"] = http.ServeContent
	decs["ServeFile"] = http.ServeFile
	decs["ServeMux"] = reflect.TypeOf((*http.ServeMux)(nil)).Elem()
	decs["ServeTLS"] = http.ServeTLS
	decs["Server"] = reflect.TypeOf((*http.Server)(nil)).Elem()
	decs["ServerContextKey"] = &http.ServerContextKey
	decs["SetCookie"] = http.SetCookie
	decs["StateActive"] = http.StateActive
	decs["StateClosed"] = http.StateClosed
	decs["StateHijacked"] = http.StateHijacked
	decs["StateIdle"] = http.StateIdle
	decs["StateNew"] = http.StateNew
	decs["StatusAccepted"] = native.UntypedNumericConst("202")
	decs["StatusAlreadyReported"] = native.UntypedNumericConst("208")
	decs["StatusBadGateway"] = native.UntypedNumericConst("502")
	decs["StatusBadRequest"] = native.UntypedNumericConst("400")
	decs["StatusConflict"] = native.UntypedNumericConst("409")
	decs["StatusContinue"] = native.UntypedNumericConst("100")
	decs["StatusCreated"] = native.UntypedNumericConst("201")
	decs["StatusEarlyHints"] = native.UntypedNumericConst("103")
	decs["StatusExpectationFailed"] = native.UntypedNumericConst("417")
	decs["StatusFailedDependency"] = native.UntypedNumericConst("424")
	decs["StatusForbidden"] = native.UntypedNumericConst("403")
	decs["StatusFound"] = native.UntypedNumericConst("302")
	decs["StatusGatewayTimeout"] = native.UntypedNumericConst("504")
	decs["StatusGone"] = native.UntypedNumericConst("410")
	decs["StatusHTTPVersionNotSupported"] = native.UntypedNumericConst("505")
	decs["StatusIMUsed"] = native.UntypedNumericConst("226")
	decs["StatusInsufficientStorage"] = native.UntypedNumericConst("507")
	decs["StatusInternalServerError"] = native.UntypedNumericConst("500")
	decs["StatusLengthRequired"] = native.UntypedNumericConst("411")
	decs["StatusLocked"] = native.UntypedNumericConst("423")
	decs["StatusLoopDetected"] = native.UntypedNumericConst("508")
	decs["StatusMethodNotAllowed"] = native.UntypedNumericConst("405")
	decs["StatusMisdirectedRequest"] = native.UntypedNumericConst("421")
	decs["StatusMovedPermanently"] = native.UntypedNumericConst("301")
	decs["StatusMultiStatus"] = native.UntypedNumericConst("207")
	decs["StatusMultipleChoices"] = native.UntypedNumericConst("300")
	decs["StatusNetworkAuthenticationRequired"] = native.UntypedNumericConst("511")
	decs["StatusNoContent"] = native.UntypedNumericConst("204")
	decs["StatusNonAuthoritativeInfo"] = native.UntypedNumericConst("203")
	decs["StatusNotAcceptable"] = native.UntypedNumericConst("406")
	decs["StatusNotExtended"] = native.UntypedNumericConst("510")
	decs["StatusNotFound"] = native.UntypedNumericConst("404")
	decs["StatusNotImplemented"] = native.UntypedNumericConst("501")
	decs["StatusNotModified"] = native.UntypedNumericConst("304")
	decs["StatusOK"] = native.UntypedNumericConst("200")
	decs["StatusPartialContent"] = native.UntypedNumericConst("206")
	decs["StatusPaymentRequired"] = native.UntypedNumericConst("402")
	decs["StatusPermanentRedirect"] = native.UntypedNumericConst("308")
	decs["StatusPreconditionFailed"] = native.UntypedNumericConst("412")
	decs["StatusPreconditionRequired"] = native.UntypedNumericConst("428")
	decs["StatusProcessing"] = native.UntypedNumericConst("102")
	decs["StatusProxyAuthRequired"] = native.UntypedNumericConst("407")
	decs["StatusRequestEntityTooLarge"] = native.UntypedNumericConst("413")
	decs["StatusRequestHeaderFieldsTooLarge"] = native.UntypedNumericConst("431")
	decs["StatusRequestTimeout"] = native.UntypedNumericConst("408")
	decs["StatusRequestURITooLong"] = native.UntypedNumericConst("414")
	decs["StatusRequestedRangeNotSatisfiable"] = native.UntypedNumericConst("416")
	decs["StatusResetContent"] = native.UntypedNumericConst("205")
	decs["StatusSeeOther"] = native.UntypedNumericConst("303")
	decs["StatusServiceUnavailable"] = native.UntypedNumericConst("503")
	decs["StatusSwitchingProtocols"] = native.UntypedNumericConst("101")
	decs["StatusTeapot"] = native.UntypedNumericConst("418")
	decs["StatusTemporaryRedirect"] = native.UntypedNumericConst("307")
	decs["StatusText"] = http.StatusText
	decs["StatusTooEarly"] = native.UntypedNumericConst("425")
	decs["StatusTooManyRequests"] = native.UntypedNumericConst("429")
	decs["StatusUnauthorized"] = native.UntypedNumericConst("401")
	decs["StatusUnavailableForLegalReasons"] = native.UntypedNumericConst("451")
	decs["StatusUnprocessableEntity"] = native.UntypedNumericConst("422")
	decs["StatusUnsupportedMediaType"] = native.UntypedNumericConst("415")
	decs["StatusUpgradeRequired"] = native.UntypedNumericConst("426")
	decs["StatusUseProxy"] = native.UntypedNumericConst("305")
	decs["StatusVariantAlsoNegotiates"] = native.UntypedNumericConst("506")
	decs["StripPrefix"] = http.StripPrefix
	decs["TimeFormat"] = native.UntypedStringConst("Mon, 02 Jan 2006 15:04:05 GMT")
	decs["TimeoutHandler"] = http.TimeoutHandler
	decs["TrailerPrefix"] = native.UntypedStringConst("Trailer:")
	decs["Transport"] = reflect.TypeOf((*http.Transport)(nil)).Elem()
	packages["net/http"] = native.Package{
		Name:         "http",
		Declarations: decs,
	}
	// "net/http/cgi"
	decs = make(native.Declarations, 4)
	decs["Handler"] = reflect.TypeOf((*cgi.Handler)(nil)).Elem()
	decs["Request"] = cgi.Request
	decs["RequestFromMap"] = cgi.RequestFromMap
	decs["Serve"] = cgi.Serve
	packages["net/http/cgi"] = native.Package{
		Name:         "cgi",
		Declarations: decs,
	}
	// "net/http/cookiejar"
	decs = make(native.Declarations, 4)
	decs["Jar"] = reflect.TypeOf((*cookiejar.Jar)(nil)).Elem()
	decs["New"] = cookiejar.New
	decs["Options"] = reflect.TypeOf((*cookiejar.Options)(nil)).Elem()
	decs["PublicSuffixList"] = reflect.TypeOf((*cookiejar.PublicSuffixList)(nil)).Elem()
	packages["net/http/cookiejar"] = native.Package{
		Name:         "cookiejar",
		Declarations: decs,
	}
	// "net/http/fcgi"
	decs = make(native.Declarations, 4)
	decs["ErrConnClosed"] = &fcgi.ErrConnClosed
	decs["ErrRequestAborted"] = &fcgi.ErrRequestAborted
	decs["ProcessEnv"] = fcgi.ProcessEnv
	decs["Serve"] = fcgi.Serve
	packages["net/http/fcgi"] = native.Package{
		Name:         "fcgi",
		Declarations: decs,
	}
	// "net/http/httptest"
	decs = make(native.Declarations, 8)
	decs["DefaultRemoteAddr"] = native.UntypedStringConst("1.2.3.4")
	decs["NewRecorder"] = httptest.NewRecorder
	decs["NewRequest"] = httptest.NewRequest
	decs["NewServer"] = httptest.NewServer
	decs["NewTLSServer"] = httptest.NewTLSServer
	decs["NewUnstartedServer"] = httptest.NewUnstartedServer
	decs["ResponseRecorder"] = reflect.TypeOf((*httptest.ResponseRecorder)(nil)).Elem()
	decs["Server"] = reflect.TypeOf((*httptest.Server)(nil)).Elem()
	packages["net/http/httptest"] = native.Package{
		Name:         "httptest",
		Declarations: decs,
	}
	// "net/http/httptrace"
	decs = make(native.Declarations, 7)
	decs["ClientTrace"] = reflect.TypeOf((*httptrace.ClientTrace)(nil)).Elem()
	decs["ContextClientTrace"] = httptrace.ContextClientTrace
	decs["DNSDoneInfo"] = reflect.TypeOf((*httptrace.DNSDoneInfo)(nil)).Elem()
	decs["DNSStartInfo"] = reflect.TypeOf((*httptrace.DNSStartInfo)(nil)).Elem()
	decs["GotConnInfo"] = reflect.TypeOf((*httptrace.GotConnInfo)(nil)).Elem()
	decs["WithClientTrace"] = httptrace.WithClientTrace
	decs["WroteRequestInfo"] = reflect.TypeOf((*httptrace.WroteRequestInfo)(nil)).Elem()
	packages["net/http/httptrace"] = native.Package{
		Name:         "httptrace",
		Declarations: decs,
	}
	// "net/http/httputil"
	decs = make(native.Declarations, 17)
	decs["BufferPool"] = reflect.TypeOf((*httputil.BufferPool)(nil)).Elem()
	decs["ClientConn"] = reflect.TypeOf((*httputil.ClientConn)(nil)).Elem()
	decs["DumpRequest"] = httputil.DumpRequest
	decs["DumpRequestOut"] = httputil.DumpRequestOut
	decs["DumpResponse"] = httputil.DumpResponse
	decs["ErrClosed"] = &httputil.ErrClosed
	decs["ErrLineTooLong"] = &httputil.ErrLineTooLong
	decs["ErrPersistEOF"] = &httputil.ErrPersistEOF
	decs["ErrPipeline"] = &httputil.ErrPipeline
	decs["NewChunkedReader"] = httputil.NewChunkedReader
	decs["NewChunkedWriter"] = httputil.NewChunkedWriter
	decs["NewClientConn"] = httputil.NewClientConn
	decs["NewProxyClientConn"] = httputil.NewProxyClientConn
	decs["NewServerConn"] = httputil.NewServerConn
	decs["NewSingleHostReverseProxy"] = httputil.NewSingleHostReverseProxy
	decs["ReverseProxy"] = reflect.TypeOf((*httputil.ReverseProxy)(nil)).Elem()
	decs["ServerConn"] = reflect.TypeOf((*httputil.ServerConn)(nil)).Elem()
	packages["net/http/httputil"] = native.Package{
		Name:         "httputil",
		Declarations: decs,
	}
	// "net/http/pprof"
	decs = make(native.Declarations, 6)
	decs["Cmdline"] = pprof.Cmdline
	decs["Handler"] = pprof.Handler
	decs["Index"] = pprof.Index
	decs["Profile"] = pprof.Profile
	decs["Symbol"] = pprof.Symbol
	decs["Trace"] = pprof.Trace
	packages["net/http/pprof"] = native.Package{
		Name:         "pprof",
		Declarations: decs,
	}
	// "net/mail"
	decs = make(native.Declarations, 9)
	decs["Address"] = reflect.TypeOf((*mail.Address)(nil)).Elem()
	decs["AddressParser"] = reflect.TypeOf((*mail.AddressParser)(nil)).Elem()
	decs["ErrHeaderNotPresent"] = &mail.ErrHeaderNotPresent
	decs["Header"] = reflect.TypeOf((*mail.Header)(nil)).Elem()
	decs["Message"] = reflect.TypeOf((*mail.Message)(nil)).Elem()
	decs["ParseAddress"] = mail.ParseAddress
	decs["ParseAddressList"] = mail.ParseAddressList
	decs["ParseDate"] = mail.ParseDate
	decs["ReadMessage"] = mail.ReadMessage
	packages["net/mail"] = native.Package{
		Name:         "mail",
		Declarations: decs,
	}
	// "net/rpc"
	decs = make(native.Declarations, 25)
	decs["Accept"] = rpc.Accept
	decs["Call"] = reflect.TypeOf((*rpc.Call)(nil)).Elem()
	decs["Client"] = reflect.TypeOf((*rpc.Client)(nil)).Elem()
	decs["ClientCodec"] = reflect.TypeOf((*rpc.ClientCodec)(nil)).Elem()
	decs["DefaultDebugPath"] = native.UntypedStringConst("/debug/rpc")
	decs["DefaultRPCPath"] = native.UntypedStringConst("/_goRPC_")
	decs["DefaultServer"] = &rpc.DefaultServer
	decs["Dial"] = rpc.Dial
	decs["DialHTTP"] = rpc.DialHTTP
	decs["DialHTTPPath"] = rpc.DialHTTPPath
	decs["ErrShutdown"] = &rpc.ErrShutdown
	decs["HandleHTTP"] = rpc.HandleHTTP
	decs["NewClient"] = rpc.NewClient
	decs["NewClientWithCodec"] = rpc.NewClientWithCodec
	decs["NewServer"] = rpc.NewServer
	decs["Register"] = rpc.Register
	decs["RegisterName"] = rpc.RegisterName
	decs["Request"] = reflect.TypeOf((*rpc.Request)(nil)).Elem()
	decs["Response"] = reflect.TypeOf((*rpc.Response)(nil)).Elem()
	decs["ServeCodec"] = rpc.ServeCodec
	decs["ServeConn"] = rpc.ServeConn
	decs["ServeRequest"] = rpc.ServeRequest
	decs["Server"] = reflect.TypeOf((*rpc.Server)(nil)).Elem()
	decs["ServerCodec"] = reflect.TypeOf((*rpc.ServerCodec)(nil)).Elem()
	decs["ServerError"] = reflect.TypeOf((*rpc.ServerError)(nil)).Elem()
	packages["net/rpc"] = native.Package{
		Name:         "rpc",
		Declarations: decs,
	}
	// "net/rpc/jsonrpc"
	decs = make(native.Declarations, 5)
	decs["Dial"] = jsonrpc.Dial
	decs["NewClient"] = jsonrpc.NewClient
	decs["NewClientCodec"] = jsonrpc.NewClientCodec
	decs["NewServerCodec"] = jsonrpc.NewServerCodec
	decs["ServeConn"] = jsonrpc.ServeConn
	packages["net/rpc/jsonrpc"] = native.Package{
		Name:         "jsonrpc",
		Declarations: decs,
	}
	// "net/smtp"
	decs = make(native.Declarations, 8)
	decs["Auth"] = reflect.TypeOf((*smtp.Auth)(nil)).Elem()
	decs["CRAMMD5Auth"] = smtp.CRAMMD5Auth
	decs["Client"] = reflect.TypeOf((*smtp.Client)(nil)).Elem()
	decs["Dial"] = smtp.Dial
	decs["NewClient"] = smtp.NewClient
	decs["PlainAuth"] = smtp.PlainAuth
	decs["SendMail"] = smtp.SendMail
	decs["ServerInfo"] = reflect.TypeOf((*smtp.ServerInfo)(nil)).Elem()
	packages["net/smtp"] = native.Package{
		Name:         "smtp",
		Declarations: decs,
	}
	// "net/textproto"
	decs = make(native.Declarations, 14)
	decs["CanonicalMIMEHeaderKey"] = textproto.CanonicalMIMEHeaderKey
	decs["Conn"] = reflect.TypeOf((*textproto.Conn)(nil)).Elem()
	decs["Dial"] = textproto.Dial
	decs["Error"] = reflect.TypeOf((*textproto.Error)(nil)).Elem()
	decs["MIMEHeader"] = reflect.TypeOf((*textproto.MIMEHeader)(nil)).Elem()
	decs["NewConn"] = textproto.NewConn
	decs["NewReader"] = textproto.NewReader
	decs["NewWriter"] = textproto.NewWriter
	decs["Pipeline"] = reflect.TypeOf((*textproto.Pipeline)(nil)).Elem()
	decs["ProtocolError"] = reflect.TypeOf((*textproto.ProtocolError)(nil)).Elem()
	decs["Reader"] = reflect.TypeOf((*textproto.Reader)(nil)).Elem()
	decs["TrimBytes"] = textproto.TrimBytes
	decs["TrimString"] = textproto.TrimString
	decs["Writer"] = reflect.TypeOf((*textproto.Writer)(nil)).Elem()
	packages["net/textproto"] = native.Package{
		Name:         "textproto",
		Declarations: decs,
	}
	// "net/url"
	decs = make(native.Declarations, 15)
	decs["Error"] = reflect.TypeOf((*url.Error)(nil)).Elem()
	decs["EscapeError"] = reflect.TypeOf((*url.EscapeError)(nil)).Elem()
	decs["InvalidHostError"] = reflect.TypeOf((*url.InvalidHostError)(nil)).Elem()
	decs["Parse"] = url.Parse
	decs["ParseQuery"] = url.ParseQuery
	decs["ParseRequestURI"] = url.ParseRequestURI
	decs["PathEscape"] = url.PathEscape
	decs["PathUnescape"] = url.PathUnescape
	decs["QueryEscape"] = url.QueryEscape
	decs["QueryUnescape"] = url.QueryUnescape
	decs["URL"] = reflect.TypeOf((*url.URL)(nil)).Elem()
	decs["User"] = url.User
	decs["UserPassword"] = url.UserPassword
	decs["Userinfo"] = reflect.TypeOf((*url.Userinfo)(nil)).Elem()
	decs["Values"] = reflect.TypeOf((*url.Values)(nil)).Elem()
	packages["net/url"] = native.Package{
		Name:         "url",
		Declarations: decs,
	}
	// "os"
	decs = make(native.Declarations, 114)
	decs["Args"] = &os.Args
	decs["Chdir"] = os.Chdir
	decs["Chmod"] = os.Chmod
	decs["Chown"] = os.Chown
	decs["Chtimes"] = os.Chtimes
	decs["Clearenv"] = os.Clearenv
	decs["Create"] = os.Create
	decs["CreateTemp"] = os.CreateTemp
	decs["DevNull"] = native.UntypedStringConst("/dev/null")
	decs["DirEntry"] = reflect.TypeOf((*os.DirEntry)(nil)).Elem()
	decs["DirFS"] = os.DirFS
	decs["Environ"] = os.Environ
	decs["ErrClosed"] = &os.ErrClosed
	decs["ErrDeadlineExceeded"] = &os.ErrDeadlineExceeded
	decs["ErrExist"] = &os.ErrExist
	decs["ErrInvalid"] = &os.ErrInvalid
	decs["ErrNoDeadline"] = &os.ErrNoDeadline
	decs["ErrNotExist"] = &os.ErrNotExist
	decs["ErrPermission"] = &os.ErrPermission
	decs["ErrProcessDone"] = &os.ErrProcessDone
	decs["Executable"] = os.Executable
	decs["Exit"] = os.Exit
	decs["Expand"] = os.Expand
	decs["ExpandEnv"] = os.ExpandEnv
	decs["File"] = reflect.TypeOf((*os.File)(nil)).Elem()
	decs["FileInfo"] = reflect.TypeOf((*os.FileInfo)(nil)).Elem()
	decs["FileMode"] = reflect.TypeOf((*os.FileMode)(nil)).Elem()
	decs["FindProcess"] = os.FindProcess
	decs["Getegid"] = os.Getegid
	decs["Getenv"] = os.Getenv
	decs["Geteuid"] = os.Geteuid
	decs["Getgid"] = os.Getgid
	decs["Getgroups"] = os.Getgroups
	decs["Getpagesize"] = os.Getpagesize
	decs["Getpid"] = os.Getpid
	decs["Getppid"] = os.Getppid
	decs["Getuid"] = os.Getuid
	decs["Getwd"] = os.Getwd
	decs["Hostname"] = os.Hostname
	decs["Interrupt"] = &os.Interrupt
	decs["IsExist"] = os.IsExist
	decs["IsNotExist"] = os.IsNotExist
	decs["IsPathSeparator"] = os.IsPathSeparator
	decs["IsPermission"] = os.IsPermission
	decs["IsTimeout"] = os.IsTimeout
	decs["Kill"] = &os.Kill
	decs["Lchown"] = os.Lchown
	decs["Link"] = os.Link
	decs["LinkError"] = reflect.TypeOf((*os.LinkError)(nil)).Elem()
	decs["LookupEnv"] = os.LookupEnv
	decs["Lstat"] = os.Lstat
	decs["Mkdir"] = os.Mkdir
	decs["MkdirAll"] = os.MkdirAll
	decs["MkdirTemp"] = os.MkdirTemp
	decs["ModeAppend"] = os.ModeAppend
	decs["ModeCharDevice"] = os.ModeCharDevice
	decs["ModeDevice"] = os.ModeDevice
	decs["ModeDir"] = os.ModeDir
	decs["ModeExclusive"] = os.ModeExclusive
	decs["ModeIrregular"] = os.ModeIrregular
	decs["ModeNamedPipe"] = os.ModeNamedPipe
	decs["ModePerm"] = os.ModePerm
	decs["ModeSetgid"] = os.ModeSetgid
	decs["ModeSetuid"] = os.ModeSetuid
	decs["ModeSocket"] = os.ModeSocket
	decs["ModeSticky"] = os.ModeSticky
	decs["ModeSymlink"] = os.ModeSymlink
	decs["ModeTemporary"] = os.ModeTemporary
	decs["ModeType"] = os.ModeType
	decs["NewFile"] = os.NewFile
	decs["NewSyscallError"] = os.NewSyscallError
	decs["O_APPEND"] = os.O_APPEND
	decs["O_CREATE"] = os.O_CREATE
	decs["O_EXCL"] = os.O_EXCL
	decs["O_RDONLY"] = os.O_RDONLY
	decs["O_RDWR"] = os.O_RDWR
	decs["O_SYNC"] = os.O_SYNC
	decs["O_TRUNC"] = os.O_TRUNC
	decs["O_WRONLY"] = os.O_WRONLY
	decs["Open"] = os.Open
	decs["OpenFile"] = os.OpenFile
	decs["PathError"] = reflect.TypeOf((*os.PathError)(nil)).Elem()
	decs["PathListSeparator"] = native.UntypedNumericConst("58")
	decs["PathSeparator"] = native.UntypedNumericConst("47")
	decs["Pipe"] = os.Pipe
	decs["ProcAttr"] = reflect.TypeOf((*os.ProcAttr)(nil)).Elem()
	decs["Process"] = reflect.TypeOf((*os.Process)(nil)).Elem()
	decs["ProcessState"] = reflect.TypeOf((*os.ProcessState)(nil)).Elem()
	decs["ReadDir"] = os.ReadDir
	decs["ReadFile"] = os.ReadFile
	decs["Readlink"] = os.Readlink
	decs["Remove"] = os.Remove
	decs["RemoveAll"] = os.RemoveAll
	decs["Rename"] = os.Rename
	decs["SEEK_CUR"] = os.SEEK_CUR
	decs["SEEK_END"] = os.SEEK_END
	decs["SEEK_SET"] = os.SEEK_SET
	decs["SameFile"] = os.SameFile
	decs["Setenv"] = os.Setenv
	decs["Signal"] = reflect.TypeOf((*os.Signal)(nil)).Elem()
	decs["StartProcess"] = os.StartProcess
	decs["Stat"] = os.Stat
	decs["Stderr"] = &os.Stderr
	decs["Stdin"] = &os.Stdin
	decs["Stdout"] = &os.Stdout
	decs["Symlink"] = os.Symlink
	decs["SyscallError"] = reflect.TypeOf((*os.SyscallError)(nil)).Elem()
	decs["TempDir"] = os.TempDir
	decs["Truncate"] = os.Truncate
	decs["Unsetenv"] = os.Unsetenv
	decs["UserCacheDir"] = os.UserCacheDir
	decs["UserConfigDir"] = os.UserConfigDir
	decs["UserHomeDir"] = os.UserHomeDir
	decs["WriteFile"] = os.WriteFile
	packages["os"] = native.Package{
		Name:         "os",
		Declarations: decs,
	}
	// "os/exec"
	decs = make(native.Declarations, 7)
	decs["Cmd"] = reflect.TypeOf((*exec.Cmd)(nil)).Elem()
	decs["Command"] = exec.Command
	decs["CommandContext"] = exec.CommandContext
	decs["ErrNotFound"] = &exec.ErrNotFound
	decs["Error"] = reflect.TypeOf((*exec.Error)(nil)).Elem()
	decs["ExitError"] = reflect.TypeOf((*exec.ExitError)(nil)).Elem()
	decs["LookPath"] = exec.LookPath
	packages["os/exec"] = native.Package{
		Name:         "exec",
		Declarations: decs,
	}
	// "os/user"
	decs = make(native.Declarations, 11)
	decs["Current"] = user.Current
	decs["Group"] = reflect.TypeOf((*user.Group)(nil)).Elem()
	decs["Lookup"] = user.Lookup
	decs["LookupGroup"] = user.LookupGroup
	decs["LookupGroupId"] = user.LookupGroupId
	decs["LookupId"] = user.LookupId
	decs["UnknownGroupError"] = reflect.TypeOf((*user.UnknownGroupError)(nil)).Elem()
	decs["UnknownGroupIdError"] = reflect.TypeOf((*user.UnknownGroupIdError)(nil)).Elem()
	decs["UnknownUserError"] = reflect.TypeOf((*user.UnknownUserError)(nil)).Elem()
	decs["UnknownUserIdError"] = reflect.TypeOf((*user.UnknownUserIdError)(nil)).Elem()
	decs["User"] = reflect.TypeOf((*user.User)(nil)).Elem()
	packages["os/user"] = native.Package{
		Name:         "user",
		Declarations: decs,
	}
	// "path"
	decs = make(native.Declarations, 9)
	decs["Base"] = path.Base
	decs["Clean"] = path.Clean
	decs["Dir"] = path.Dir
	decs["ErrBadPattern"] = &path.ErrBadPattern
	decs["Ext"] = path.Ext
	decs["IsAbs"] = path.IsAbs
	decs["Join"] = path.Join
	decs["Match"] = path.Match
	decs["Split"] = path.Split
	packages["path"] = native.Package{
		Name:         "path",
		Declarations: decs,
	}
	// "path/filepath"
	decs = make(native.Declarations, 24)
	decs["Abs"] = filepath.Abs
	decs["Base"] = filepath.Base
	decs["Clean"] = filepath.Clean
	decs["Dir"] = filepath.Dir
	decs["ErrBadPattern"] = &filepath.ErrBadPattern
	decs["EvalSymlinks"] = filepath.EvalSymlinks
	decs["Ext"] = filepath.Ext
	decs["FromSlash"] = filepath.FromSlash
	decs["Glob"] = filepath.Glob
	decs["HasPrefix"] = filepath.HasPrefix
	decs["IsAbs"] = filepath.IsAbs
	decs["Join"] = filepath.Join
	decs["ListSeparator"] = native.UntypedNumericConst("58")
	decs["Match"] = filepath.Match
	decs["Rel"] = filepath.Rel
	decs["Separator"] = native.UntypedNumericConst("47")
	decs["SkipDir"] = &filepath.SkipDir
	decs["Split"] = filepath.Split
	decs["SplitList"] = filepath.SplitList
	decs["ToSlash"] = filepath.ToSlash
	decs["VolumeName"] = filepath.VolumeName
	decs["Walk"] = filepath.Walk
	decs["WalkDir"] = filepath.WalkDir
	decs["WalkFunc"] = reflect.TypeOf((*filepath.WalkFunc)(nil)).Elem()
	packages["path/filepath"] = native.Package{
		Name:         "filepath",
		Declarations: decs,
	}
	// "reflect"
	decs = make(native.Declarations, 70)
	decs["Append"] = reflect.Append
	decs["AppendSlice"] = reflect.AppendSlice
	decs["Array"] = reflect.Array
	decs["ArrayOf"] = reflect.ArrayOf
	decs["Bool"] = reflect.Bool
	decs["BothDir"] = reflect.BothDir
	decs["Chan"] = reflect.Chan
	decs["ChanDir"] = reflect.TypeOf((*reflect.ChanDir)(nil)).Elem()
	decs["ChanOf"] = reflect.ChanOf
	decs["Complex128"] = reflect.Complex128
	decs["Complex64"] = reflect.Complex64
	decs["Copy"] = reflect.Copy
	decs["DeepEqual"] = reflect.DeepEqual
	decs["Float32"] = reflect.Float32
	decs["Float64"] = reflect.Float64
	decs["Func"] = reflect.Func
	decs["FuncOf"] = reflect.FuncOf
	decs["Indirect"] = reflect.Indirect
	decs["Int"] = reflect.Int
	decs["Int16"] = reflect.Int16
	decs["Int32"] = reflect.Int32
	decs["Int64"] = reflect.Int64
	decs["Int8"] = reflect.Int8
	decs["Interface"] = reflect.Interface
	decs["Invalid"] = reflect.Invalid
	decs["Kind"] = reflect.TypeOf((*reflect.Kind)(nil)).Elem()
	decs["MakeChan"] = reflect.MakeChan
	decs["MakeFunc"] = reflect.MakeFunc
	decs["MakeMap"] = reflect.MakeMap
	decs["MakeMapWithSize"] = reflect.MakeMapWithSize
	decs["MakeSlice"] = reflect.MakeSlice
	decs["Map"] = reflect.Map
	decs["MapIter"] = reflect.TypeOf((*reflect.MapIter)(nil)).Elem()
	decs["MapOf"] = reflect.MapOf
	decs["Method"] = reflect.TypeOf((*reflect.Method)(nil)).Elem()
	decs["New"] = reflect.New
	decs["NewAt"] = reflect.NewAt
	decs["Ptr"] = reflect.Ptr
	decs["PtrTo"] = reflect.PtrTo
	decs["RecvDir"] = reflect.RecvDir
	decs["Select"] = reflect.Select
	decs["SelectCase"] = reflect.TypeOf((*reflect.SelectCase)(nil)).Elem()
	decs["SelectDefault"] = reflect.SelectDefault
	decs["SelectDir"] = reflect.TypeOf((*reflect.SelectDir)(nil)).Elem()
	decs["SelectRecv"] = reflect.SelectRecv
	decs["SelectSend"] = reflect.SelectSend
	decs["SendDir"] = reflect.SendDir
	decs["Slice"] = reflect.Slice
	decs["SliceHeader"] = reflect.TypeOf((*reflect.SliceHeader)(nil)).Elem()
	decs["SliceOf"] = reflect.SliceOf
	decs["String"] = reflect.String
	decs["StringHeader"] = reflect.TypeOf((*reflect.StringHeader)(nil)).Elem()
	decs["Struct"] = reflect.Struct
	decs["StructField"] = reflect.TypeOf((*reflect.StructField)(nil)).Elem()
	decs["StructOf"] = reflect.StructOf
	decs["StructTag"] = reflect.TypeOf((*reflect.StructTag)(nil)).Elem()
	decs["Swapper"] = reflect.Swapper
	decs["Type"] = reflect.TypeOf((*reflect.Type)(nil)).Elem()
	decs["TypeOf"] = reflect.TypeOf
	decs["Uint"] = reflect.Uint
	decs["Uint16"] = reflect.Uint16
	decs["Uint32"] = reflect.Uint32
	decs["Uint64"] = reflect.Uint64
	decs["Uint8"] = reflect.Uint8
	decs["Uintptr"] = reflect.Uintptr
	decs["UnsafePointer"] = reflect.UnsafePointer
	decs["Value"] = reflect.TypeOf((*reflect.Value)(nil)).Elem()
	decs["ValueError"] = reflect.TypeOf((*reflect.ValueError)(nil)).Elem()
	decs["ValueOf"] = reflect.ValueOf
	decs["Zero"] = reflect.Zero
	packages["reflect"] = native.Package{
		Name:         "reflect",
		Declarations: decs,
	}
	// "regexp"
	decs = make(native.Declarations, 9)
	decs["Compile"] = regexp.Compile
	decs["CompilePOSIX"] = regexp.CompilePOSIX
	decs["Match"] = regexp.Match
	decs["MatchReader"] = regexp.MatchReader
	decs["MatchString"] = regexp.MatchString
	decs["MustCompile"] = regexp.MustCompile
	decs["MustCompilePOSIX"] = regexp.MustCompilePOSIX
	decs["QuoteMeta"] = regexp.QuoteMeta
	decs["Regexp"] = reflect.TypeOf((*regexp.Regexp)(nil)).Elem()
	packages["regexp"] = native.Package{
		Name:         "regexp",
		Declarations: decs,
	}
	// "regexp/syntax"
	decs = make(native.Declarations, 76)
	decs["ClassNL"] = syntax.ClassNL
	decs["Compile"] = syntax.Compile
	decs["DotNL"] = syntax.DotNL
	decs["EmptyBeginLine"] = syntax.EmptyBeginLine
	decs["EmptyBeginText"] = syntax.EmptyBeginText
	decs["EmptyEndLine"] = syntax.EmptyEndLine
	decs["EmptyEndText"] = syntax.EmptyEndText
	decs["EmptyNoWordBoundary"] = syntax.EmptyNoWordBoundary
	decs["EmptyOp"] = reflect.TypeOf((*syntax.EmptyOp)(nil)).Elem()
	decs["EmptyOpContext"] = syntax.EmptyOpContext
	decs["EmptyWordBoundary"] = syntax.EmptyWordBoundary
	decs["ErrInternalError"] = syntax.ErrInternalError
	decs["ErrInvalidCharClass"] = syntax.ErrInvalidCharClass
	decs["ErrInvalidCharRange"] = syntax.ErrInvalidCharRange
	decs["ErrInvalidEscape"] = syntax.ErrInvalidEscape
	decs["ErrInvalidNamedCapture"] = syntax.ErrInvalidNamedCapture
	decs["ErrInvalidPerlOp"] = syntax.ErrInvalidPerlOp
	decs["ErrInvalidRepeatOp"] = syntax.ErrInvalidRepeatOp
	decs["ErrInvalidRepeatSize"] = syntax.ErrInvalidRepeatSize
	decs["ErrInvalidUTF8"] = syntax.ErrInvalidUTF8
	decs["ErrMissingBracket"] = syntax.ErrMissingBracket
	decs["ErrMissingParen"] = syntax.ErrMissingParen
	decs["ErrMissingRepeatArgument"] = syntax.ErrMissingRepeatArgument
	decs["ErrTrailingBackslash"] = syntax.ErrTrailingBackslash
	decs["ErrUnexpectedParen"] = syntax.ErrUnexpectedParen
	decs["Error"] = reflect.TypeOf((*syntax.Error)(nil)).Elem()
	decs["ErrorCode"] = reflect.TypeOf((*syntax.ErrorCode)(nil)).Elem()
	decs["Flags"] = reflect.TypeOf((*syntax.Flags)(nil)).Elem()
	decs["FoldCase"] = syntax.FoldCase
	decs["Inst"] = reflect.TypeOf((*syntax.Inst)(nil)).Elem()
	decs["InstAlt"] = syntax.InstAlt
	decs["InstAltMatch"] = syntax.InstAltMatch
	decs["InstCapture"] = syntax.InstCapture
	decs["InstEmptyWidth"] = syntax.InstEmptyWidth
	decs["InstFail"] = syntax.InstFail
	decs["InstMatch"] = syntax.InstMatch
	decs["InstNop"] = syntax.InstNop
	decs["InstOp"] = reflect.TypeOf((*syntax.InstOp)(nil)).Elem()
	decs["InstRune"] = syntax.InstRune
	decs["InstRune1"] = syntax.InstRune1
	decs["InstRuneAny"] = syntax.InstRuneAny
	decs["InstRuneAnyNotNL"] = syntax.InstRuneAnyNotNL
	decs["IsWordChar"] = syntax.IsWordChar
	decs["Literal"] = syntax.Literal
	decs["MatchNL"] = syntax.MatchNL
	decs["NonGreedy"] = syntax.NonGreedy
	decs["OneLine"] = syntax.OneLine
	decs["Op"] = reflect.TypeOf((*syntax.Op)(nil)).Elem()
	decs["OpAlternate"] = syntax.OpAlternate
	decs["OpAnyChar"] = syntax.OpAnyChar
	decs["OpAnyCharNotNL"] = syntax.OpAnyCharNotNL
	decs["OpBeginLine"] = syntax.OpBeginLine
	decs["OpBeginText"] = syntax.OpBeginText
	decs["OpCapture"] = syntax.OpCapture
	decs["OpCharClass"] = syntax.OpCharClass
	decs["OpConcat"] = syntax.OpConcat
	decs["OpEmptyMatch"] = syntax.OpEmptyMatch
	decs["OpEndLine"] = syntax.OpEndLine
	decs["OpEndText"] = syntax.OpEndText
	decs["OpLiteral"] = syntax.OpLiteral
	decs["OpNoMatch"] = syntax.OpNoMatch
	decs["OpNoWordBoundary"] = syntax.OpNoWordBoundary
	decs["OpPlus"] = syntax.OpPlus
	decs["OpQuest"] = syntax.OpQuest
	decs["OpRepeat"] = syntax.OpRepeat
	decs["OpStar"] = syntax.OpStar
	decs["OpWordBoundary"] = syntax.OpWordBoundary
	decs["POSIX"] = syntax.POSIX
	decs["Parse"] = syntax.Parse
	decs["Perl"] = syntax.Perl
	decs["PerlX"] = syntax.PerlX
	decs["Prog"] = reflect.TypeOf((*syntax.Prog)(nil)).Elem()
	decs["Regexp"] = reflect.TypeOf((*syntax.Regexp)(nil)).Elem()
	decs["Simple"] = syntax.Simple
	decs["UnicodeGroups"] = syntax.UnicodeGroups
	decs["WasDollar"] = syntax.WasDollar
	packages["regexp/syntax"] = native.Package{
		Name:         "syntax",
		Declarations: decs,
	}
	// "runtime/debug"
	decs = make(native.Declarations, 14)
	decs["BuildInfo"] = reflect.TypeOf((*debug.BuildInfo)(nil)).Elem()
	decs["FreeOSMemory"] = debug.FreeOSMemory
	decs["GCStats"] = reflect.TypeOf((*debug.GCStats)(nil)).Elem()
	decs["Module"] = reflect.TypeOf((*debug.Module)(nil)).Elem()
	decs["PrintStack"] = debug.PrintStack
	decs["ReadBuildInfo"] = debug.ReadBuildInfo
	decs["ReadGCStats"] = debug.ReadGCStats
	decs["SetGCPercent"] = debug.SetGCPercent
	decs["SetMaxStack"] = debug.SetMaxStack
	decs["SetMaxThreads"] = debug.SetMaxThreads
	decs["SetPanicOnFault"] = debug.SetPanicOnFault
	decs["SetTraceback"] = debug.SetTraceback
	decs["Stack"] = debug.Stack
	decs["WriteHeapDump"] = debug.WriteHeapDump
	packages["runtime/debug"] = native.Package{
		Name:         "debug",
		Declarations: decs,
	}
	// "sort"
	decs = make(native.Declarations, 21)
	decs["Float64Slice"] = reflect.TypeOf((*sort.Float64Slice)(nil)).Elem()
	decs["Float64s"] = sort.Float64s
	decs["Float64sAreSorted"] = sort.Float64sAreSorted
	decs["IntSlice"] = reflect.TypeOf((*sort.IntSlice)(nil)).Elem()
	decs["Interface"] = reflect.TypeOf((*sort.Interface)(nil)).Elem()
	decs["Ints"] = sort.Ints
	decs["IntsAreSorted"] = sort.IntsAreSorted
	decs["IsSorted"] = sort.IsSorted
	decs["Reverse"] = sort.Reverse
	decs["Search"] = sort.Search
	decs["SearchFloat64s"] = sort.SearchFloat64s
	decs["SearchInts"] = sort.SearchInts
	decs["SearchStrings"] = sort.SearchStrings
	decs["Slice"] = sort.Slice
	decs["SliceIsSorted"] = sort.SliceIsSorted
	decs["SliceStable"] = sort.SliceStable
	decs["Sort"] = sort.Sort
	decs["Stable"] = sort.Stable
	decs["StringSlice"] = reflect.TypeOf((*sort.StringSlice)(nil)).Elem()
	decs["Strings"] = sort.Strings
	decs["StringsAreSorted"] = sort.StringsAreSorted
	packages["sort"] = native.Package{
		Name:         "sort",
		Declarations: decs,
	}
	// "strconv"
	decs = make(native.Declarations, 37)
	decs["AppendBool"] = strconv.AppendBool
	decs["AppendFloat"] = strconv.AppendFloat
	decs["AppendInt"] = strconv.AppendInt
	decs["AppendQuote"] = strconv.AppendQuote
	decs["AppendQuoteRune"] = strconv.AppendQuoteRune
	decs["AppendQuoteRuneToASCII"] = strconv.AppendQuoteRuneToASCII
	decs["AppendQuoteRuneToGraphic"] = strconv.AppendQuoteRuneToGraphic
	decs["AppendQuoteToASCII"] = strconv.AppendQuoteToASCII
	decs["AppendQuoteToGraphic"] = strconv.AppendQuoteToGraphic
	decs["AppendUint"] = strconv.AppendUint
	decs["Atoi"] = strconv.Atoi
	decs["CanBackquote"] = strconv.CanBackquote
	decs["ErrRange"] = &strconv.ErrRange
	decs["ErrSyntax"] = &strconv.ErrSyntax
	decs["FormatBool"] = strconv.FormatBool
	decs["FormatComplex"] = strconv.FormatComplex
	decs["FormatFloat"] = strconv.FormatFloat
	decs["FormatInt"] = strconv.FormatInt
	decs["FormatUint"] = strconv.FormatUint
	decs["IntSize"] = native.UntypedNumericConst("64")
	decs["IsGraphic"] = strconv.IsGraphic
	decs["IsPrint"] = strconv.IsPrint
	decs["Itoa"] = strconv.Itoa
	decs["NumError"] = reflect.TypeOf((*strconv.NumError)(nil)).Elem()
	decs["ParseBool"] = strconv.ParseBool
	decs["ParseComplex"] = strconv.ParseComplex
	decs["ParseFloat"] = strconv.ParseFloat
	decs["ParseInt"] = strconv.ParseInt
	decs["ParseUint"] = strconv.ParseUint
	decs["Quote"] = strconv.Quote
	decs["QuoteRune"] = strconv.QuoteRune
	decs["QuoteRuneToASCII"] = strconv.QuoteRuneToASCII
	decs["QuoteRuneToGraphic"] = strconv.QuoteRuneToGraphic
	decs["QuoteToASCII"] = strconv.QuoteToASCII
	decs["QuoteToGraphic"] = strconv.QuoteToGraphic
	decs["Unquote"] = strconv.Unquote
	decs["UnquoteChar"] = strconv.UnquoteChar
	packages["strconv"] = native.Package{
		Name:         "strconv",
		Declarations: decs,
	}
	// "strings"
	decs = make(native.Declarations, 50)
	decs["Builder"] = reflect.TypeOf((*strings.Builder)(nil)).Elem()
	decs["Compare"] = strings.Compare
	decs["Contains"] = strings.Contains
	decs["ContainsAny"] = strings.ContainsAny
	decs["ContainsRune"] = strings.ContainsRune
	decs["Count"] = strings.Count
	decs["EqualFold"] = strings.EqualFold
	decs["Fields"] = strings.Fields
	decs["FieldsFunc"] = strings.FieldsFunc
	decs["HasPrefix"] = strings.HasPrefix
	decs["HasSuffix"] = strings.HasSuffix
	decs["Index"] = strings.Index
	decs["IndexAny"] = strings.IndexAny
	decs["IndexByte"] = strings.IndexByte
	decs["IndexFunc"] = strings.IndexFunc
	decs["IndexRune"] = strings.IndexRune
	decs["Join"] = strings.Join
	decs["LastIndex"] = strings.LastIndex
	decs["LastIndexAny"] = strings.LastIndexAny
	decs["LastIndexByte"] = strings.LastIndexByte
	decs["LastIndexFunc"] = strings.LastIndexFunc
	decs["Map"] = strings.Map
	decs["NewReader"] = strings.NewReader
	decs["NewReplacer"] = strings.NewReplacer
	decs["Reader"] = reflect.TypeOf((*strings.Reader)(nil)).Elem()
	decs["Repeat"] = strings.Repeat
	decs["Replace"] = strings.Replace
	decs["ReplaceAll"] = strings.ReplaceAll
	decs["Replacer"] = reflect.TypeOf((*strings.Replacer)(nil)).Elem()
	decs["Split"] = strings.Split
	decs["SplitAfter"] = strings.SplitAfter
	decs["SplitAfterN"] = strings.SplitAfterN
	decs["SplitN"] = strings.SplitN
	decs["Title"] = strings.Title
	decs["ToLower"] = strings.ToLower
	decs["ToLowerSpecial"] = strings.ToLowerSpecial
	decs["ToTitle"] = strings.ToTitle
	decs["ToTitleSpecial"] = strings.ToTitleSpecial
	decs["ToUpper"] = strings.ToUpper
	decs["ToUpperSpecial"] = strings.ToUpperSpecial
	decs["ToValidUTF8"] = strings.ToValidUTF8
	decs["Trim"] = strings.Trim
	decs["TrimFunc"] = strings.TrimFunc
	decs["TrimLeft"] = strings.TrimLeft
	decs["TrimLeftFunc"] = strings.TrimLeftFunc
	decs["TrimPrefix"] = strings.TrimPrefix
	decs["TrimRight"] = strings.TrimRight
	decs["TrimRightFunc"] = strings.TrimRightFunc
	decs["TrimSpace"] = strings.TrimSpace
	decs["TrimSuffix"] = strings.TrimSuffix
	packages["strings"] = native.Package{
		Name:         "strings",
		Declarations: decs,
	}
	// "sync"
	decs = make(native.Declarations, 9)
	decs["Cond"] = reflect.TypeOf((*sync.Cond)(nil)).Elem()
	decs["Locker"] = reflect.TypeOf((*sync.Locker)(nil)).Elem()
	decs["Map"] = reflect.TypeOf((*sync.Map)(nil)).Elem()
	decs["Mutex"] = reflect.TypeOf((*sync.Mutex)(nil)).Elem()
	decs["NewCond"] = sync.NewCond
	decs["Once"] = reflect.TypeOf((*sync.Once)(nil)).Elem()
	decs["Pool"] = reflect.TypeOf((*sync.Pool)(nil)).Elem()
	decs["RWMutex"] = reflect.TypeOf((*sync.RWMutex)(nil)).Elem()
	decs["WaitGroup"] = reflect.TypeOf((*sync.WaitGroup)(nil)).Elem()
	packages["sync"] = native.Package{
		Name:         "sync",
		Declarations: decs,
	}
	// "sync/atomic"
	decs = make(native.Declarations, 30)
	decs["AddInt32"] = atomic.AddInt32
	decs["AddInt64"] = atomic.AddInt64
	decs["AddUint32"] = atomic.AddUint32
	decs["AddUint64"] = atomic.AddUint64
	decs["AddUintptr"] = atomic.AddUintptr
	decs["CompareAndSwapInt32"] = atomic.CompareAndSwapInt32
	decs["CompareAndSwapInt64"] = atomic.CompareAndSwapInt64
	decs["CompareAndSwapPointer"] = atomic.CompareAndSwapPointer
	decs["CompareAndSwapUint32"] = atomic.CompareAndSwapUint32
	decs["CompareAndSwapUint64"] = atomic.CompareAndSwapUint64
	decs["CompareAndSwapUintptr"] = atomic.CompareAndSwapUintptr
	decs["LoadInt32"] = atomic.LoadInt32
	decs["LoadInt64"] = atomic.LoadInt64
	decs["LoadPointer"] = atomic.LoadPointer
	decs["LoadUint32"] = atomic.LoadUint32
	decs["LoadUint64"] = atomic.LoadUint64
	decs["LoadUintptr"] = atomic.LoadUintptr
	decs["StoreInt32"] = atomic.StoreInt32
	decs["StoreInt64"] = atomic.StoreInt64
	decs["StorePointer"] = atomic.StorePointer
	decs["StoreUint32"] = atomic.StoreUint32
	decs["StoreUint64"] = atomic.StoreUint64
	decs["StoreUintptr"] = atomic.StoreUintptr
	decs["SwapInt32"] = atomic.SwapInt32
	decs["SwapInt64"] = atomic.SwapInt64
	decs["SwapPointer"] = atomic.SwapPointer
	decs["SwapUint32"] = atomic.SwapUint32
	decs["SwapUint64"] = atomic.SwapUint64
	decs["SwapUintptr"] = atomic.SwapUintptr
	decs["Value"] = reflect.TypeOf((*atomic.Value)(nil)).Elem()
	packages["sync/atomic"] = native.Package{
		Name:         "atomic",
		Declarations: decs,
	}
	// "text/scanner"
	decs = make(native.Declarations, 21)
	decs["Char"] = native.UntypedNumericConst("-5")
	decs["Comment"] = native.UntypedNumericConst("-8")
	decs["EOF"] = native.UntypedNumericConst("-1")
	decs["Float"] = native.UntypedNumericConst("-4")
	decs["GoTokens"] = native.UntypedNumericConst("1012")
	decs["GoWhitespace"] = native.UntypedNumericConst("4294977024")
	decs["Ident"] = native.UntypedNumericConst("-2")
	decs["Int"] = native.UntypedNumericConst("-3")
	decs["Position"] = reflect.TypeOf((*scanner_.Position)(nil)).Elem()
	decs["RawString"] = native.UntypedNumericConst("-7")
	decs["ScanChars"] = native.UntypedNumericConst("32")
	decs["ScanComments"] = native.UntypedNumericConst("256")
	decs["ScanFloats"] = native.UntypedNumericConst("16")
	decs["ScanIdents"] = native.UntypedNumericConst("4")
	decs["ScanInts"] = native.UntypedNumericConst("8")
	decs["ScanRawStrings"] = native.UntypedNumericConst("128")
	decs["ScanStrings"] = native.UntypedNumericConst("64")
	decs["Scanner"] = reflect.TypeOf((*scanner_.Scanner)(nil)).Elem()
	decs["SkipComments"] = native.UntypedNumericConst("512")
	decs["String"] = native.UntypedNumericConst("-6")
	decs["TokenString"] = scanner_.TokenString
	packages["text/scanner"] = native.Package{
		Name:         "scanner",
		Declarations: decs,
	}
	// "text/tabwriter"
	decs = make(native.Declarations, 9)
	decs["AlignRight"] = tabwriter.AlignRight
	decs["Debug"] = tabwriter.Debug
	decs["DiscardEmptyColumns"] = tabwriter.DiscardEmptyColumns
	decs["Escape"] = native.UntypedNumericConst("255")
	decs["FilterHTML"] = tabwriter.FilterHTML
	decs["NewWriter"] = tabwriter.NewWriter
	decs["StripEscape"] = tabwriter.StripEscape
	decs["TabIndent"] = tabwriter.TabIndent
	decs["Writer"] = reflect.TypeOf((*tabwriter.Writer)(nil)).Elem()
	packages["text/tabwriter"] = native.Package{
		Name:         "tabwriter",
		Declarations: decs,
	}
	// "text/template"
	decs = make(native.Declarations, 16)
	decs["ExecError"] = reflect.TypeOf((*template_.ExecError)(nil)).Elem()
	decs["FuncMap"] = reflect.TypeOf((*template_.FuncMap)(nil)).Elem()
	decs["HTMLEscape"] = template_.HTMLEscape
	decs["HTMLEscapeString"] = template_.HTMLEscapeString
	decs["HTMLEscaper"] = template_.HTMLEscaper
	decs["IsTrue"] = template_.IsTrue
	decs["JSEscape"] = template_.JSEscape
	decs["JSEscapeString"] = template_.JSEscapeString
	decs["JSEscaper"] = template_.JSEscaper
	decs["Must"] = template_.Must
	decs["New"] = template_.New
	decs["ParseFS"] = template_.ParseFS
	decs["ParseFiles"] = template_.ParseFiles
	decs["ParseGlob"] = template_.ParseGlob
	decs["Template"] = reflect.TypeOf((*template_.Template)(nil)).Elem()
	decs["URLQueryEscaper"] = template_.URLQueryEscaper
	packages["text/template"] = native.Package{
		Name:         "template",
		Declarations: decs,
	}
	// "text/template/parse"
	decs = make(native.Declarations, 49)
	decs["ActionNode"] = reflect.TypeOf((*parse.ActionNode)(nil)).Elem()
	decs["BoolNode"] = reflect.TypeOf((*parse.BoolNode)(nil)).Elem()
	decs["BranchNode"] = reflect.TypeOf((*parse.BranchNode)(nil)).Elem()
	decs["ChainNode"] = reflect.TypeOf((*parse.ChainNode)(nil)).Elem()
	decs["CommandNode"] = reflect.TypeOf((*parse.CommandNode)(nil)).Elem()
	decs["CommentNode"] = reflect.TypeOf((*parse.CommentNode)(nil)).Elem()
	decs["DotNode"] = reflect.TypeOf((*parse.DotNode)(nil)).Elem()
	decs["FieldNode"] = reflect.TypeOf((*parse.FieldNode)(nil)).Elem()
	decs["IdentifierNode"] = reflect.TypeOf((*parse.IdentifierNode)(nil)).Elem()
	decs["IfNode"] = reflect.TypeOf((*parse.IfNode)(nil)).Elem()
	decs["IsEmptyTree"] = parse.IsEmptyTree
	decs["ListNode"] = reflect.TypeOf((*parse.ListNode)(nil)).Elem()
	decs["Mode"] = reflect.TypeOf((*parse.Mode)(nil)).Elem()
	decs["New"] = parse.New
	decs["NewIdentifier"] = parse.NewIdentifier
	decs["NilNode"] = reflect.TypeOf((*parse.NilNode)(nil)).Elem()
	decs["Node"] = reflect.TypeOf((*parse.Node)(nil)).Elem()
	decs["NodeAction"] = parse.NodeAction
	decs["NodeBool"] = parse.NodeBool
	decs["NodeChain"] = parse.NodeChain
	decs["NodeCommand"] = parse.NodeCommand
	decs["NodeComment"] = parse.NodeComment
	decs["NodeDot"] = parse.NodeDot
	decs["NodeField"] = parse.NodeField
	decs["NodeIdentifier"] = parse.NodeIdentifier
	decs["NodeIf"] = parse.NodeIf
	decs["NodeList"] = parse.NodeList
	decs["NodeNil"] = parse.NodeNil
	decs["NodeNumber"] = parse.NodeNumber
	decs["NodePipe"] = parse.NodePipe
	decs["NodeRange"] = parse.NodeRange
	decs["NodeString"] = parse.NodeString
	decs["NodeTemplate"] = parse.NodeTemplate
	decs["NodeText"] = parse.NodeText
	decs["NodeType"] = reflect.TypeOf((*parse.NodeType)(nil)).Elem()
	decs["NodeVariable"] = parse.NodeVariable
	decs["NodeWith"] = parse.NodeWith
	decs["NumberNode"] = reflect.TypeOf((*parse.NumberNode)(nil)).Elem()
	decs["Parse"] = parse.Parse
	decs["ParseComments"] = parse.ParseComments
	decs["PipeNode"] = reflect.TypeOf((*parse.PipeNode)(nil)).Elem()
	decs["Pos"] = reflect.TypeOf((*parse.Pos)(nil)).Elem()
	decs["RangeNode"] = reflect.TypeOf((*parse.RangeNode)(nil)).Elem()
	decs["StringNode"] = reflect.TypeOf((*parse.StringNode)(nil)).Elem()
	decs["TemplateNode"] = reflect.TypeOf((*parse.TemplateNode)(nil)).Elem()
	decs["TextNode"] = reflect.TypeOf((*parse.TextNode)(nil)).Elem()
	decs["Tree"] = reflect.TypeOf((*parse.Tree)(nil)).Elem()
	decs["VariableNode"] = reflect.TypeOf((*parse.VariableNode)(nil)).Elem()
	decs["WithNode"] = reflect.TypeOf((*parse.WithNode)(nil)).Elem()
	packages["text/template/parse"] = native.Package{
		Name:         "parse",
		Declarations: decs,
	}
	// "time"
	decs = make(native.Declarations, 67)
	decs["ANSIC"] = native.UntypedStringConst("Mon Jan _2 15:04:05 2006")
	decs["After"] = time.After
	decs["AfterFunc"] = time.AfterFunc
	decs["April"] = time.April
	decs["August"] = time.August
	decs["Date"] = time.Date
	decs["December"] = time.December
	decs["Duration"] = reflect.TypeOf((*time.Duration)(nil)).Elem()
	decs["February"] = time.February
	decs["FixedZone"] = time.FixedZone
	decs["Friday"] = time.Friday
	decs["Hour"] = time.Hour
	decs["January"] = time.January
	decs["July"] = time.July
	decs["June"] = time.June
	decs["Kitchen"] = native.UntypedStringConst("3:04PM")
	decs["LoadLocation"] = time.LoadLocation
	decs["LoadLocationFromTZData"] = time.LoadLocationFromTZData
	decs["Local"] = &time.Local
	decs["Location"] = reflect.TypeOf((*time.Location)(nil)).Elem()
	decs["March"] = time.March
	decs["May"] = time.May
	decs["Microsecond"] = time.Microsecond
	decs["Millisecond"] = time.Millisecond
	decs["Minute"] = time.Minute
	decs["Monday"] = time.Monday
	decs["Month"] = reflect.TypeOf((*time.Month)(nil)).Elem()
	decs["Nanosecond"] = time.Nanosecond
	decs["NewTicker"] = time.NewTicker
	decs["NewTimer"] = time.NewTimer
	decs["November"] = time.November
	decs["Now"] = time.Now
	decs["October"] = time.October
	decs["Parse"] = time.Parse
	decs["ParseDuration"] = time.ParseDuration
	decs["ParseError"] = reflect.TypeOf((*time.ParseError)(nil)).Elem()
	decs["ParseInLocation"] = time.ParseInLocation
	decs["RFC1123"] = native.UntypedStringConst("Mon, 02 Jan 2006 15:04:05 MST")
	decs["RFC1123Z"] = native.UntypedStringConst("Mon, 02 Jan 2006 15:04:05 -0700")
	decs["RFC3339"] = native.UntypedStringConst("2006-01-02T15:04:05Z07:00")
	decs["RFC3339Nano"] = native.UntypedStringConst("2006-01-02T15:04:05.999999999Z07:00")
	decs["RFC822"] = native.UntypedStringConst("02 Jan 06 15:04 MST")
	decs["RFC822Z"] = native.UntypedStringConst("02 Jan 06 15:04 -0700")
	decs["RFC850"] = native.UntypedStringConst("Monday, 02-Jan-06 15:04:05 MST")
	decs["RubyDate"] = native.UntypedStringConst("Mon Jan 02 15:04:05 -0700 2006")
	decs["Saturday"] = time.Saturday
	decs["Second"] = time.Second
	decs["September"] = time.September
	decs["Since"] = time.Since
	decs["Sleep"] = time.Sleep
	decs["Stamp"] = native.UntypedStringConst("Jan _2 15:04:05")
	decs["StampMicro"] = native.UntypedStringConst("Jan _2 15:04:05.000000")
	decs["StampMilli"] = native.UntypedStringConst("Jan _2 15:04:05.000")
	decs["StampNano"] = native.UntypedStringConst("Jan _2 15:04:05.000000000")
	decs["Sunday"] = time.Sunday
	decs["Thursday"] = time.Thursday
	decs["Tick"] = time.Tick
	decs["Ticker"] = reflect.TypeOf((*time.Ticker)(nil)).Elem()
	decs["Time"] = reflect.TypeOf((*time.Time)(nil)).Elem()
	decs["Timer"] = reflect.TypeOf((*time.Timer)(nil)).Elem()
	decs["Tuesday"] = time.Tuesday
	decs["UTC"] = &time.UTC
	decs["Unix"] = time.Unix
	decs["UnixDate"] = native.UntypedStringConst("Mon Jan _2 15:04:05 MST 2006")
	decs["Until"] = time.Until
	decs["Wednesday"] = time.Wednesday
	decs["Weekday"] = reflect.TypeOf((*time.Weekday)(nil)).Elem()
	packages["time"] = native.Package{
		Name:         "time",
		Declarations: decs,
	}
	// "unicode"
	decs = make(native.Declarations, 284)
	decs["ASCII_Hex_Digit"] = &unicode.ASCII_Hex_Digit
	decs["Adlam"] = &unicode.Adlam
	decs["Ahom"] = &unicode.Ahom
	decs["Anatolian_Hieroglyphs"] = &unicode.Anatolian_Hieroglyphs
	decs["Arabic"] = &unicode.Arabic
	decs["Armenian"] = &unicode.Armenian
	decs["Avestan"] = &unicode.Avestan
	decs["AzeriCase"] = &unicode.AzeriCase
	decs["Balinese"] = &unicode.Balinese
	decs["Bamum"] = &unicode.Bamum
	decs["Bassa_Vah"] = &unicode.Bassa_Vah
	decs["Batak"] = &unicode.Batak
	decs["Bengali"] = &unicode.Bengali
	decs["Bhaiksuki"] = &unicode.Bhaiksuki
	decs["Bidi_Control"] = &unicode.Bidi_Control
	decs["Bopomofo"] = &unicode.Bopomofo
	decs["Brahmi"] = &unicode.Brahmi
	decs["Braille"] = &unicode.Braille
	decs["Buginese"] = &unicode.Buginese
	decs["Buhid"] = &unicode.Buhid
	decs["C"] = &unicode.C
	decs["Canadian_Aboriginal"] = &unicode.Canadian_Aboriginal
	decs["Carian"] = &unicode.Carian
	decs["CaseRange"] = reflect.TypeOf((*unicode.CaseRange)(nil)).Elem()
	decs["CaseRanges"] = &unicode.CaseRanges
	decs["Categories"] = &unicode.Categories
	decs["Caucasian_Albanian"] = &unicode.Caucasian_Albanian
	decs["Cc"] = &unicode.Cc
	decs["Cf"] = &unicode.Cf
	decs["Chakma"] = &unicode.Chakma
	decs["Cham"] = &unicode.Cham
	decs["Cherokee"] = &unicode.Cherokee
	decs["Chorasmian"] = &unicode.Chorasmian
	decs["Co"] = &unicode.Co
	decs["Common"] = &unicode.Common
	decs["Coptic"] = &unicode.Coptic
	decs["Cs"] = &unicode.Cs
	decs["Cuneiform"] = &unicode.Cuneiform
	decs["Cypriot"] = &unicode.Cypriot
	decs["Cyrillic"] = &unicode.Cyrillic
	decs["Dash"] = &unicode.Dash
	decs["Deprecated"] = &unicode.Deprecated
	decs["Deseret"] = &unicode.Deseret
	decs["Devanagari"] = &unicode.Devanagari
	decs["Diacritic"] = &unicode.Diacritic
	decs["Digit"] = &unicode.Digit
	decs["Dives_Akuru"] = &unicode.Dives_Akuru
	decs["Dogra"] = &unicode.Dogra
	decs["Duployan"] = &unicode.Duployan
	decs["Egyptian_Hieroglyphs"] = &unicode.Egyptian_Hieroglyphs
	decs["Elbasan"] = &unicode.Elbasan
	decs["Elymaic"] = &unicode.Elymaic
	decs["Ethiopic"] = &unicode.Ethiopic
	decs["Extender"] = &unicode.Extender
	decs["FoldCategory"] = &unicode.FoldCategory
	decs["FoldScript"] = &unicode.FoldScript
	decs["Georgian"] = &unicode.Georgian
	decs["Glagolitic"] = &unicode.Glagolitic
	decs["Gothic"] = &unicode.Gothic
	decs["Grantha"] = &unicode.Grantha
	decs["GraphicRanges"] = &unicode.GraphicRanges
	decs["Greek"] = &unicode.Greek
	decs["Gujarati"] = &unicode.Gujarati
	decs["Gunjala_Gondi"] = &unicode.Gunjala_Gondi
	decs["Gurmukhi"] = &unicode.Gurmukhi
	decs["Han"] = &unicode.Han
	decs["Hangul"] = &unicode.Hangul
	decs["Hanifi_Rohingya"] = &unicode.Hanifi_Rohingya
	decs["Hanunoo"] = &unicode.Hanunoo
	decs["Hatran"] = &unicode.Hatran
	decs["Hebrew"] = &unicode.Hebrew
	decs["Hex_Digit"] = &unicode.Hex_Digit
	decs["Hiragana"] = &unicode.Hiragana
	decs["Hyphen"] = &unicode.Hyphen
	decs["IDS_Binary_Operator"] = &unicode.IDS_Binary_Operator
	decs["IDS_Trinary_Operator"] = &unicode.IDS_Trinary_Operator
	decs["Ideographic"] = &unicode.Ideographic
	decs["Imperial_Aramaic"] = &unicode.Imperial_Aramaic
	decs["In"] = unicode.In
	decs["Inherited"] = &unicode.Inherited
	decs["Inscriptional_Pahlavi"] = &unicode.Inscriptional_Pahlavi
	decs["Inscriptional_Parthian"] = &unicode.Inscriptional_Parthian
	decs["Is"] = unicode.Is
	decs["IsControl"] = unicode.IsControl
	decs["IsDigit"] = unicode.IsDigit
	decs["IsGraphic"] = unicode.IsGraphic
	decs["IsLetter"] = unicode.IsLetter
	decs["IsLower"] = unicode.IsLower
	decs["IsMark"] = unicode.IsMark
	decs["IsNumber"] = unicode.IsNumber
	decs["IsOneOf"] = unicode.IsOneOf
	decs["IsPrint"] = unicode.IsPrint
	decs["IsPunct"] = unicode.IsPunct
	decs["IsSpace"] = unicode.IsSpace
	decs["IsSymbol"] = unicode.IsSymbol
	decs["IsTitle"] = unicode.IsTitle
	decs["IsUpper"] = unicode.IsUpper
	decs["Javanese"] = &unicode.Javanese
	decs["Join_Control"] = &unicode.Join_Control
	decs["Kaithi"] = &unicode.Kaithi
	decs["Kannada"] = &unicode.Kannada
	decs["Katakana"] = &unicode.Katakana
	decs["Kayah_Li"] = &unicode.Kayah_Li
	decs["Kharoshthi"] = &unicode.Kharoshthi
	decs["Khitan_Small_Script"] = &unicode.Khitan_Small_Script
	decs["Khmer"] = &unicode.Khmer
	decs["Khojki"] = &unicode.Khojki
	decs["Khudawadi"] = &unicode.Khudawadi
	decs["L"] = &unicode.L
	decs["Lao"] = &unicode.Lao
	decs["Latin"] = &unicode.Latin
	decs["Lepcha"] = &unicode.Lepcha
	decs["Letter"] = &unicode.Letter
	decs["Limbu"] = &unicode.Limbu
	decs["Linear_A"] = &unicode.Linear_A
	decs["Linear_B"] = &unicode.Linear_B
	decs["Lisu"] = &unicode.Lisu
	decs["Ll"] = &unicode.Ll
	decs["Lm"] = &unicode.Lm
	decs["Lo"] = &unicode.Lo
	decs["Logical_Order_Exception"] = &unicode.Logical_Order_Exception
	decs["Lower"] = &unicode.Lower
	decs["LowerCase"] = native.UntypedNumericConst("1")
	decs["Lt"] = &unicode.Lt
	decs["Lu"] = &unicode.Lu
	decs["Lycian"] = &unicode.Lycian
	decs["Lydian"] = &unicode.Lydian
	decs["M"] = &unicode.M
	decs["Mahajani"] = &unicode.Mahajani
	decs["Makasar"] = &unicode.Makasar
	decs["Malayalam"] = &unicode.Malayalam
	decs["Mandaic"] = &unicode.Mandaic
	decs["Manichaean"] = &unicode.Manichaean
	decs["Marchen"] = &unicode.Marchen
	decs["Mark"] = &unicode.Mark
	decs["Masaram_Gondi"] = &unicode.Masaram_Gondi
	decs["MaxASCII"] = native.UntypedNumericConst("127")
	decs["MaxCase"] = native.UntypedNumericConst("3")
	decs["MaxLatin1"] = native.UntypedNumericConst("255")
	decs["MaxRune"] = native.UntypedNumericConst("1114111")
	decs["Mc"] = &unicode.Mc
	decs["Me"] = &unicode.Me
	decs["Medefaidrin"] = &unicode.Medefaidrin
	decs["Meetei_Mayek"] = &unicode.Meetei_Mayek
	decs["Mende_Kikakui"] = &unicode.Mende_Kikakui
	decs["Meroitic_Cursive"] = &unicode.Meroitic_Cursive
	decs["Meroitic_Hieroglyphs"] = &unicode.Meroitic_Hieroglyphs
	decs["Miao"] = &unicode.Miao
	decs["Mn"] = &unicode.Mn
	decs["Modi"] = &unicode.Modi
	decs["Mongolian"] = &unicode.Mongolian
	decs["Mro"] = &unicode.Mro
	decs["Multani"] = &unicode.Multani
	decs["Myanmar"] = &unicode.Myanmar
	decs["N"] = &unicode.N
	decs["Nabataean"] = &unicode.Nabataean
	decs["Nandinagari"] = &unicode.Nandinagari
	decs["Nd"] = &unicode.Nd
	decs["New_Tai_Lue"] = &unicode.New_Tai_Lue
	decs["Newa"] = &unicode.Newa
	decs["Nko"] = &unicode.Nko
	decs["Nl"] = &unicode.Nl
	decs["No"] = &unicode.No
	decs["Noncharacter_Code_Point"] = &unicode.Noncharacter_Code_Point
	decs["Number"] = &unicode.Number
	decs["Nushu"] = &unicode.Nushu
	decs["Nyiakeng_Puachue_Hmong"] = &unicode.Nyiakeng_Puachue_Hmong
	decs["Ogham"] = &unicode.Ogham
	decs["Ol_Chiki"] = &unicode.Ol_Chiki
	decs["Old_Hungarian"] = &unicode.Old_Hungarian
	decs["Old_Italic"] = &unicode.Old_Italic
	decs["Old_North_Arabian"] = &unicode.Old_North_Arabian
	decs["Old_Permic"] = &unicode.Old_Permic
	decs["Old_Persian"] = &unicode.Old_Persian
	decs["Old_Sogdian"] = &unicode.Old_Sogdian
	decs["Old_South_Arabian"] = &unicode.Old_South_Arabian
	decs["Old_Turkic"] = &unicode.Old_Turkic
	decs["Oriya"] = &unicode.Oriya
	decs["Osage"] = &unicode.Osage
	decs["Osmanya"] = &unicode.Osmanya
	decs["Other"] = &unicode.Other
	decs["Other_Alphabetic"] = &unicode.Other_Alphabetic
	decs["Other_Default_Ignorable_Code_Point"] = &unicode.Other_Default_Ignorable_Code_Point
	decs["Other_Grapheme_Extend"] = &unicode.Other_Grapheme_Extend
	decs["Other_ID_Continue"] = &unicode.Other_ID_Continue
	decs["Other_ID_Start"] = &unicode.Other_ID_Start
	decs["Other_Lowercase"] = &unicode.Other_Lowercase
	decs["Other_Math"] = &unicode.Other_Math
	decs["Other_Uppercase"] = &unicode.Other_Uppercase
	decs["P"] = &unicode.P
	decs["Pahawh_Hmong"] = &unicode.Pahawh_Hmong
	decs["Palmyrene"] = &unicode.Palmyrene
	decs["Pattern_Syntax"] = &unicode.Pattern_Syntax
	decs["Pattern_White_Space"] = &unicode.Pattern_White_Space
	decs["Pau_Cin_Hau"] = &unicode.Pau_Cin_Hau
	decs["Pc"] = &unicode.Pc
	decs["Pd"] = &unicode.Pd
	decs["Pe"] = &unicode.Pe
	decs["Pf"] = &unicode.Pf
	decs["Phags_Pa"] = &unicode.Phags_Pa
	decs["Phoenician"] = &unicode.Phoenician
	decs["Pi"] = &unicode.Pi
	decs["Po"] = &unicode.Po
	decs["Prepended_Concatenation_Mark"] = &unicode.Prepended_Concatenation_Mark
	decs["PrintRanges"] = &unicode.PrintRanges
	decs["Properties"] = &unicode.Properties
	decs["Ps"] = &unicode.Ps
	decs["Psalter_Pahlavi"] = &unicode.Psalter_Pahlavi
	decs["Punct"] = &unicode.Punct
	decs["Quotation_Mark"] = &unicode.Quotation_Mark
	decs["Radical"] = &unicode.Radical
	decs["Range16"] = reflect.TypeOf((*unicode.Range16)(nil)).Elem()
	decs["Range32"] = reflect.TypeOf((*unicode.Range32)(nil)).Elem()
	decs["RangeTable"] = reflect.TypeOf((*unicode.RangeTable)(nil)).Elem()
	decs["Regional_Indicator"] = &unicode.Regional_Indicator
	decs["Rejang"] = &unicode.Rejang
	decs["ReplacementChar"] = native.UntypedNumericConst("65533")
	decs["Runic"] = &unicode.Runic
	decs["S"] = &unicode.S
	decs["STerm"] = &unicode.STerm
	decs["Samaritan"] = &unicode.Samaritan
	decs["Saurashtra"] = &unicode.Saurashtra
	decs["Sc"] = &unicode.Sc
	decs["Scripts"] = &unicode.Scripts
	decs["Sentence_Terminal"] = &unicode.Sentence_Terminal
	decs["Sharada"] = &unicode.Sharada
	decs["Shavian"] = &unicode.Shavian
	decs["Siddham"] = &unicode.Siddham
	decs["SignWriting"] = &unicode.SignWriting
	decs["SimpleFold"] = unicode.SimpleFold
	decs["Sinhala"] = &unicode.Sinhala
	decs["Sk"] = &unicode.Sk
	decs["Sm"] = &unicode.Sm
	decs["So"] = &unicode.So
	decs["Soft_Dotted"] = &unicode.Soft_Dotted
	decs["Sogdian"] = &unicode.Sogdian
	decs["Sora_Sompeng"] = &unicode.Sora_Sompeng
	decs["Soyombo"] = &unicode.Soyombo
	decs["Space"] = &unicode.Space
	decs["SpecialCase"] = reflect.TypeOf((*unicode.SpecialCase)(nil)).Elem()
	decs["Sundanese"] = &unicode.Sundanese
	decs["Syloti_Nagri"] = &unicode.Syloti_Nagri
	decs["Symbol"] = &unicode.Symbol
	decs["Syriac"] = &unicode.Syriac
	decs["Tagalog"] = &unicode.Tagalog
	decs["Tagbanwa"] = &unicode.Tagbanwa
	decs["Tai_Le"] = &unicode.Tai_Le
	decs["Tai_Tham"] = &unicode.Tai_Tham
	decs["Tai_Viet"] = &unicode.Tai_Viet
	decs["Takri"] = &unicode.Takri
	decs["Tamil"] = &unicode.Tamil
	decs["Tangut"] = &unicode.Tangut
	decs["Telugu"] = &unicode.Telugu
	decs["Terminal_Punctuation"] = &unicode.Terminal_Punctuation
	decs["Thaana"] = &unicode.Thaana
	decs["Thai"] = &unicode.Thai
	decs["Tibetan"] = &unicode.Tibetan
	decs["Tifinagh"] = &unicode.Tifinagh
	decs["Tirhuta"] = &unicode.Tirhuta
	decs["Title"] = &unicode.Title
	decs["TitleCase"] = native.UntypedNumericConst("2")
	decs["To"] = unicode.To
	decs["ToLower"] = unicode.ToLower
	decs["ToTitle"] = unicode.ToTitle
	decs["ToUpper"] = unicode.ToUpper
	decs["TurkishCase"] = &unicode.TurkishCase
	decs["Ugaritic"] = &unicode.Ugaritic
	decs["Unified_Ideograph"] = &unicode.Unified_Ideograph
	decs["Upper"] = &unicode.Upper
	decs["UpperCase"] = native.UntypedNumericConst("0")
	decs["UpperLower"] = native.UntypedNumericConst("1114112")
	decs["Vai"] = &unicode.Vai
	decs["Variation_Selector"] = &unicode.Variation_Selector
	decs["Version"] = native.UntypedStringConst("13.0.0")
	decs["Wancho"] = &unicode.Wancho
	decs["Warang_Citi"] = &unicode.Warang_Citi
	decs["White_Space"] = &unicode.White_Space
	decs["Yezidi"] = &unicode.Yezidi
	decs["Yi"] = &unicode.Yi
	decs["Z"] = &unicode.Z
	decs["Zanabazar_Square"] = &unicode.Zanabazar_Square
	decs["Zl"] = &unicode.Zl
	decs["Zp"] = &unicode.Zp
	decs["Zs"] = &unicode.Zs
	packages["unicode"] = native.Package{
		Name:         "unicode",
		Declarations: decs,
	}
	// "unicode/utf16"
	decs = make(native.Declarations, 5)
	decs["Decode"] = utf16.Decode
	decs["DecodeRune"] = utf16.DecodeRune
	decs["Encode"] = utf16.Encode
	decs["EncodeRune"] = utf16.EncodeRune
	decs["IsSurrogate"] = utf16.IsSurrogate
	packages["unicode/utf16"] = native.Package{
		Name:         "utf16",
		Declarations: decs,
	}
	// "unicode/utf8"
	decs = make(native.Declarations, 18)
	decs["DecodeLastRune"] = utf8.DecodeLastRune
	decs["DecodeLastRuneInString"] = utf8.DecodeLastRuneInString
	decs["DecodeRune"] = utf8.DecodeRune
	decs["DecodeRuneInString"] = utf8.DecodeRuneInString
	decs["EncodeRune"] = utf8.EncodeRune
	decs["FullRune"] = utf8.FullRune
	decs["FullRuneInString"] = utf8.FullRuneInString
	decs["MaxRune"] = native.UntypedNumericConst("1114111")
	decs["RuneCount"] = utf8.RuneCount
	decs["RuneCountInString"] = utf8.RuneCountInString
	decs["RuneError"] = native.UntypedNumericConst("65533")
	decs["RuneLen"] = utf8.RuneLen
	decs["RuneSelf"] = native.UntypedNumericConst("128")
	decs["RuneStart"] = utf8.RuneStart
	decs["UTFMax"] = native.UntypedNumericConst("4")
	decs["Valid"] = utf8.Valid
	decs["ValidRune"] = utf8.ValidRune
	decs["ValidString"] = utf8.ValidString
	packages["unicode/utf8"] = native.Package{
		Name:         "utf8",
		Declarations: decs,
	}
}

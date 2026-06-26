namespace Effect

/// Configuration for the S2 service. The first strawman intentionally exposes
/// only stable client options; advanced endpoint/retry/session configuration can
/// be added without changing the service shape.
type S2Config =
    { AccessToken: string
      RequestTimeoutMillis: int option
      ConnectionTimeoutMillis: int option }

    static member Create(accessToken: string) =
        { AccessToken = accessToken
          RequestTimeoutMillis = None
          ConnectionTimeoutMillis = None }

/// A normalized S2 SDK error carried on the typed error channel.
type S2Error =
    { Name: string
      Message: string
      Code: string option
      Status: int option
      Origin: string option
      Data: obj option }

type S2PageRequest =
    { Prefix: string option
      StartAfter: string option
      Limit: int option }


    static member Empty =
        { Prefix = None
          StartAfter = None
          Limit = None }

type S2StreamRef = { Basin: string; Stream: string }

type S2RequestOptions =
    { Signal: obj option }


    static member Empty = { Signal = None }

type S2StreamPosition = { SeqNum: float; Timestamp: DateTime }

type S2TailResponse = { Tail: S2StreamPosition }

type S2AppendOptions =
    { MatchSeqNum: float option
      FencingToken: string option }


    static member Empty =
        { MatchSeqNum = None
          FencingToken = None }

type S2StringAppendRecord =
    { Body: string
      Headers: (string * string) list
      Timestamp: DateTime option }

type S2BytesAppendRecord =
    { Body: byte[]
      Headers: (byte[] * byte[]) list
      Timestamp: DateTime option }

type S2AppendRecord =
    | StringRecord of S2StringAppendRecord
    | BytesRecord of S2BytesAppendRecord

type S2AppendRequest =
    { Target: S2StreamRef
      Records: S2AppendRecord list
      Options: S2AppendOptions option
      RequestOptions: S2RequestOptions option }

type S2AppendAck =
    { Start: S2StreamPosition
      End: S2StreamPosition
      Tail: S2StreamPosition }

type S2ReadStart =
    | FromSeqNum of float
    | FromTimestamp of DateTime
    | FromTailOffset of int

type S2ReadLimits =
    { Count: int option
      Bytes: int option }


    static member Empty = { Count = None; Bytes = None }

type S2ReadStop =
    { Limits: S2ReadLimits option
      UntilTimestamp: DateTime option
      WaitSeconds: int option }


    static member Empty =
        { Limits = None
          UntilTimestamp = None
          WaitSeconds = None }

type S2ReadFormat =
    | ReadString
    | ReadBytes

type S2ReadRequest =
    { Target: S2StreamRef
      Start: S2ReadStart option
      Clamp: bool option
      Stop: S2ReadStop option
      IgnoreCommandRecords: bool option
      Format: S2ReadFormat
      RequestOptions: S2RequestOptions option }

type S2RecordBody =
    | StringBody of string
    | BytesBody of byte[]

type S2RecordHeader =
    | StringHeader of string * string
    | BytesHeader of byte[] * byte[]

type S2ReadRecord =
    { SeqNum: float
      Body: S2RecordBody
      Headers: S2RecordHeader list
      Timestamp: DateTime }

type S2ReadBatch =
    { Records: S2ReadRecord list
      Tail: S2StreamPosition option }

type S2BasinInfo =
    { Name: string
      Location: string option
      CreatedAt: DateTime
      DeletedAt: DateTime option }

type S2ListBasinsResponse =
    { Basins: S2BasinInfo list
      HasMore: bool }

type S2StreamInfo =
    { Name: string
      CreatedAt: DateTime
      DeletedAt: DateTime option
      Cipher: string option }

type S2ListStreamsResponse =
    { Streams: S2StreamInfo list
      HasMore: bool }

type S2CreateStreamRequest =
    { Basin: string
      Stream: string
      Config: obj option
      RequestOptions: S2RequestOptions option }

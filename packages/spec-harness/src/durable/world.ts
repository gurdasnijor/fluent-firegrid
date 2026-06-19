import { AttachmentContentEncoding } from "@cucumber/messages"

/**
 * Minimal scenario `World` exposing Cucumber's `attach` / `log` / `link`
 * surface. One world instance per scenario carries world-local state across
 * steps; captured attachments accumulate in a single list and the worker
 * attributes each to the step that produced it by slicing on length.
 */

export interface CapturedAttachment {
  readonly body: string
  readonly mediaType: string
  readonly contentEncoding: AttachmentContentEncoding
  readonly fileName?: string
}

export interface AttachOptions {
  readonly mediaType: string
  readonly fileName?: string
}

export interface World {
  attach(data: unknown, options: string | AttachOptions): Promise<void>
  log(text: string): Promise<void>
  link(uri: string): Promise<void>
}

const LOG_MEDIA_TYPE = "text/x.cucumber.log+plain"
const URI_LIST_MEDIA_TYPE = "text/uri-list"

const isReadableStream = (value: unknown): value is NodeJS.ReadableStream =>
  typeof value === "object" && value !== null && typeof (value as { pipe?: unknown }).pipe === "function"

const streamToBuffer = (stream: NodeJS.ReadableStream): Promise<Buffer> =>
  new Promise((resolve, reject) => {
    const chunks: Array<Buffer> = []
    stream.on("data", (chunk: Buffer | string) => {
      chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk)
    })
    stream.on("end", () => resolve(Buffer.concat(chunks)))
    stream.on("error", reject)
  })

const normalizeOptions = (options: string | AttachOptions): AttachOptions =>
  typeof options === "string" ? { mediaType: options } : options

const withFileName = (
  attachment: Omit<CapturedAttachment, "fileName">,
  fileName: string | undefined,
): CapturedAttachment => (fileName === undefined ? attachment : { ...attachment, fileName })

export const makeWorld = (): { readonly world: World; readonly captured: ReadonlyArray<CapturedAttachment> } => {
  const captured: Array<CapturedAttachment> = []

  const push = (attachment: CapturedAttachment): void => {
    captured.push(attachment)
  }

  const attach = async (data: unknown, options: string | AttachOptions): Promise<void> => {
    const { mediaType, fileName } = normalizeOptions(options)
    if (typeof data === "string") {
      push(withFileName({ body: data, mediaType, contentEncoding: AttachmentContentEncoding.IDENTITY }, fileName))
      return
    }
    const buffer = isReadableStream(data)
      ? await streamToBuffer(data)
      : Buffer.from(data as Uint8Array)
    push(withFileName(
      { body: buffer.toString("base64"), mediaType, contentEncoding: AttachmentContentEncoding.BASE64 },
      fileName,
    ))
  }

  const world: World = {
    attach,
    log: (text) => attach(text, LOG_MEDIA_TYPE),
    link: (uri) => attach(uri, URI_LIST_MEDIA_TYPE),
  }

  return { world, captured }
}

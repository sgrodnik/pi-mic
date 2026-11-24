import WebSocket from "ws";
import { joinParts } from "../../core/transcription/transcript.ts";
import { DEEPGRAM_URL, type DeepgramOptions } from "./options.ts";
import { deepgramQuery } from "./query.ts";

const KEEP_ALIVE_MS = 8000;
interface ResultsMessage {
  type?: string;
  is_final?: boolean;
  channel?: { alternatives?: { transcript?: string }[] };
}

export interface DeepgramStream {
  send(pcm: Int16Array): void;
  ready(): Promise<void>;
  finish(): Promise<string>;
  cancel(): void;
}

class DeepgramStreamSession implements DeepgramStream {
  readonly #options: DeepgramOptions;
  readonly #socket: WebSocket;
  readonly #opened: Promise<void>;
  readonly #keepAlive: NodeJS.Timeout;
  readonly #finals: string[] = [];
  readonly #unsent: Uint8Array[] = [];
  #interim = "";

  constructor(options: DeepgramOptions) {
    this.#options = options;
    this.#socket = this.#connect();
    this.#opened = this.#watchOpening();
    this.#opened.catch((err) => options.onError?.(err));
    this.#socket.on("message", (data, isBinary) => this.#onMessage(data, isBinary));
    this.#keepAlive = setInterval(() => this.#sendKeepAlive(), KEEP_ALIVE_MS);
  }

  send(pcm: Int16Array): void {
    const bytes = new Uint8Array(pcm.buffer, pcm.byteOffset, pcm.byteLength).slice();
    if (this.#socket.readyState === WebSocket.OPEN) this.#socket.send(bytes);
    else if (this.#socket.readyState === WebSocket.CONNECTING) this.#unsent.push(bytes);
  }

  ready(): Promise<void> {
    return this.#opened;
  }

  async finish(): Promise<string> {
    const transcript = joinParts([...this.#finals, this.#interim]);
    this.cancel();
    return transcript;
  }

  cancel(): void {
    clearInterval(this.#keepAlive);
    this.#socket.terminate();
  }

  #connect(): WebSocket {
    const url = this.#options.url || DEEPGRAM_URL;
    const query = deepgramQuery(this.#options, {
      encoding: "linear16",
      sample_rate: String(this.#options.sampleRate),
      channels: "1",
      interim_results: "true",
      endpointing: "300",
    });
    return new WebSocket(`${url}?${query}`, {
      headers: { Authorization: `Token ${this.#options.apiKey}` },
    });
  }

  #watchOpening(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.#socket.on("open", () => {
        for (const bytes of this.#unsent.splice(0)) this.#socket.send(bytes);
        resolve();
      });
      this.#socket.on("unexpected-response", (_req, res) => {
        reject(new Error(`Deepgram HTTP ${res.statusCode}`));
      });
      this.#socket.on("error", (err) => {
        reject(err);
      });
    });
  }

  #onMessage(data: WebSocket.RawData, isBinary: boolean): void {
    if (isBinary) return;
    const message = JSON.parse(String(data)) as ResultsMessage;
    if (message.type !== "Results") return;
    const transcript = (message.channel?.alternatives?.[0]?.transcript ?? "").trim();
    if (message.is_final) {
      this.#finals.push(transcript);
      this.#interim = "";
    } else this.#interim = transcript;
    this.#options.onUpdate(this.#streamedText(), this.#interim);
  }

  #sendKeepAlive(): void {
    if (this.#socket.readyState === WebSocket.OPEN) {
      this.#socket.send(JSON.stringify({ type: "KeepAlive" }));
    }
  }

  #streamedText(): string {
    return joinParts(this.#finals);
  }
}

export function openDeepgramStream(options: DeepgramOptions): DeepgramStream {
  return new DeepgramStreamSession(options);
}

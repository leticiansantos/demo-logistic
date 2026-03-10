import { useState, useRef } from 'react'
import { Mic, MicOff, Send, RotateCcw, Clock, Zap } from 'lucide-react'
import api from '../api/client'

// Converte WebM → WAV 16 kHz mono PCM
async function blobToWav(blob) {
  const arrayBuffer = await blob.arrayBuffer()
  const audioCtx = new AudioContext({ sampleRate: 16000 })
  const decoded = await audioCtx.decodeAudioData(arrayBuffer)
  await audioCtx.close()
  const pcm = decoded.getChannelData(0)
  const numSamples = pcm.length
  const sampleRate = 16000
  const dataSize = numSamples * 2
  const buf = new ArrayBuffer(44 + dataSize)
  const v = new DataView(buf)
  const wr = (off, s) => [...s].forEach((c, i) => v.setUint8(off + i, c.charCodeAt(0)))
  wr(0, 'RIFF'); v.setUint32(4, 36 + dataSize, true); wr(8, 'WAVE')
  wr(12, 'fmt '); v.setUint32(16, 16, true); v.setUint16(20, 1, true); v.setUint16(22, 1, true)
  v.setUint32(24, sampleRate, true); v.setUint32(28, sampleRate * 2, true)
  v.setUint16(32, 2, true); v.setUint16(34, 16, true)
  wr(36, 'data'); v.setUint32(40, dataSize, true)
  let off = 44
  for (let i = 0; i < numSamples; i++) { v.setInt16(off, Math.max(-1, Math.min(1, pcm[i])) * 32767, true); off += 2 }
  return new Blob([buf], { type: 'audio/wav' })
}

function fmtTime(s) {
  const m = Math.floor(s / 60).toString().padStart(2, '0')
  const sec = (s % 60).toString().padStart(2, '0')
  return `${m}:${sec}`
}

const ENDPOINTS = [
  { key: 'faster-whisper', label: 'Faster Whisper', model: 'faster-whisper-large-v3', color: 'bg-violet-600' },
  { key: 'whisper',        label: 'Whisper',        model: 'whisper-large-v3',         color: 'bg-blue-500'   },
]

function EndpointBadge({ endpointName }) {
  const ep = ENDPOINTS.find(e => e.model === endpointName) || ENDPOINTS[0]
  return (
    <span className={`text-[10px] font-semibold text-white px-2 py-0.5 rounded-full ${ep.color}`}>
      {ep.label}
    </span>
  )
}

export default function TranscribeTest() {
  const [recording, setRecording]         = useState(false)
  const [audioBlob, setAudioBlob]         = useState(null)
  const [audioUrl, setAudioUrl]           = useState(null)
  const [recordingTime, setRecordingTime] = useState(0)
  const [loading, setLoading]             = useState(false)
  const [result, setResult]               = useState(null)
  const [error, setError]                 = useState(null)
  const [history, setHistory]             = useState([])
  const [selectedEndpoint, setSelectedEndpoint] = useState('faster-whisper')

  const mediaRecorderRef = useRef(null)
  const chunksRef        = useRef([])
  const streamRef        = useRef(null)
  const timerRef         = useRef(null)

  const startRecording = async () => {
    try {
      chunksRef.current = []
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
      streamRef.current = stream
      const mimeType = MediaRecorder.isTypeSupported('audio/webm;codecs=opus') ? 'audio/webm;codecs=opus' : 'audio/webm'
      const mr = new MediaRecorder(stream, { mimeType })
      mr.ondataavailable = e => { if (e.data.size > 0) chunksRef.current.push(e.data) }
      mr.onstop = () => {
        const blob = new Blob(chunksRef.current, { type: mimeType })
        setAudioBlob(blob)
        setAudioUrl(URL.createObjectURL(blob))
        stream.getTracks().forEach(t => t.stop())
        streamRef.current = null
      }
      mr.start(100)
      mediaRecorderRef.current = mr
      setRecording(true)
      setRecordingTime(0)
      timerRef.current = setInterval(() => setRecordingTime(t => t + 1), 1000)
    } catch { alert('Permissão de microfone negada.') }
  }

  const stopRecording = () => {
    clearInterval(timerRef.current)
    if (mediaRecorderRef.current?.state === 'recording') mediaRecorderRef.current.stop()
    setRecording(false)
  }

  const reset = () => {
    setAudioBlob(null)
    setAudioUrl(null)
    setResult(null)
    setError(null)
    setRecordingTime(0)
  }

  const sendAudio = async () => {
    if (!audioBlob || loading) return
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      let uploadBlob = audioBlob
      try { uploadBlob = await blobToWav(audioBlob) } catch (_) { /* usa original */ }
      const form = new FormData()
      form.append('file', uploadBlob, 'audio.wav')
      const r = await api.post(`/transcribe-test?endpoint=${selectedEndpoint}`, form, {
        headers: { 'Content-Type': 'multipart/form-data' },
      })
      setResult(r.data)
      setHistory(prev => [{ ...r.data, audioUrl, ts: new Date().toLocaleTimeString('pt-BR') }, ...prev].slice(0, 10))
    } catch (e) {
      setError(e.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }

  const activeEp = ENDPOINTS.find(e => e.key === selectedEndpoint)

  return (
    <div className="max-w-2xl mx-auto p-8">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Teste de Transcrição</h1>
        <p className="text-sm text-gray-500 mt-1">Compare os endpoints de transcrição de áudio</p>
      </div>

      {/* Endpoint toggle */}
      <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-4 mb-4">
        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Endpoint</p>
        <div className="flex gap-2">
          {ENDPOINTS.map(ep => (
            <button
              key={ep.key}
              onClick={() => setSelectedEndpoint(ep.key)}
              className={`flex-1 py-2.5 px-4 rounded-xl text-sm font-semibold transition-all border ${
                selectedEndpoint === ep.key
                  ? `${ep.color} text-white border-transparent shadow-sm`
                  : 'bg-gray-50 text-gray-500 border-gray-200 hover:bg-gray-100'
              }`}
            >
              {ep.label}
              <span className={`block text-[10px] font-normal mt-0.5 ${selectedEndpoint === ep.key ? 'text-white/80' : 'text-gray-400'}`}>
                {ep.model}
              </span>
            </button>
          ))}
        </div>
      </div>

      {/* Recorder */}
      <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-6 mb-4">
        {!audioBlob ? (
          <div className="flex flex-col items-center gap-4">
            <button
              onClick={recording ? stopRecording : startRecording}
              className={`w-20 h-20 rounded-full flex items-center justify-center shadow-lg transition-all ${
                recording ? 'bg-red-500 hover:bg-red-600 animate-pulse' : `${activeEp.color} hover:opacity-90`
              }`}
            >
              {recording ? <MicOff size={32} className="text-white" /> : <Mic size={32} className="text-white" />}
            </button>
            {recording
              ? <p className="text-red-500 font-mono font-semibold text-lg">{fmtTime(recordingTime)}</p>
              : <p className="text-gray-400 text-sm">Clique para gravar</p>}
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            <audio src={audioUrl} controls className="w-full" />
            <div className="flex gap-2">
              <button
                onClick={sendAudio}
                disabled={loading}
                className={`flex-1 flex items-center justify-center gap-2 ${activeEp.color} hover:opacity-90 disabled:opacity-40 text-white font-semibold py-2.5 rounded-xl transition-all`}
              >
                {loading
                  ? <><span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" /> Transcrevendo…</>
                  : <><Send size={16} /> Transcrever com {activeEp.label}</>}
              </button>
              <button
                onClick={reset}
                disabled={loading}
                className="px-4 py-2.5 rounded-xl border border-gray-200 text-gray-500 hover:bg-gray-50 disabled:opacity-40 transition-all"
              >
                <RotateCcw size={16} />
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Result */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 mb-4">
          <p className="text-red-700 text-sm font-medium">Erro</p>
          <p className="text-red-600 text-sm mt-1 font-mono break-all">{error}</p>
        </div>
      )}

      {result && (
        <div className="bg-green-50 border border-green-200 rounded-xl p-5 mb-4">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <p className="text-green-700 text-sm font-semibold">Transcrição</p>
              <EndpointBadge endpointName={result.endpoint} />
            </div>
            <span className="flex items-center gap-1 text-xs text-green-600">
              <Clock size={11} /> {result.elapsed_s}s
            </span>
          </div>
          <p className="text-gray-800 text-base leading-relaxed">
            {result.transcript || <span className="text-gray-400 italic">(vazio)</span>}
          </p>
          <button onClick={reset} className="mt-3 text-xs text-green-600 hover:underline flex items-center gap-1">
            <RotateCcw size={11} /> Nova gravação
          </button>
        </div>
      )}

      {/* History */}
      {history.length > 0 && (
        <div className="mt-6">
          <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Histórico da sessão</p>
          <div className="flex flex-col gap-2">
            {history.map((h, i) => (
              <div key={i} className="bg-white border border-gray-100 rounded-xl p-4 flex gap-3 items-start">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <EndpointBadge endpointName={h.endpoint} />
                    <span className="text-xs text-gray-400">{h.ts}</span>
                    <span className="text-xs text-gray-400 flex items-center gap-0.5">
                      <Clock size={10} />{h.elapsed_s}s
                    </span>
                  </div>
                  <p className="text-gray-800 text-sm">
                    {h.transcript || <span className="text-gray-400 italic">(vazio)</span>}
                  </p>
                </div>
                {h.audioUrl && <audio src={h.audioUrl} controls className="h-8 w-32 flex-shrink-0" />}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

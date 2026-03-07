import { useState, useEffect, useRef } from 'react'
import { Send, Mic, Trash2, Zap, RefreshCw, ChevronDown } from 'lucide-react'
import api from '../api/client'

// Converte markdown básico (**, *, _) para HTML inline seguro (conteúdo gerado pelo bot)
function renderMarkdown(text) {
  if (!text) return ''
  return text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(?!\*)(.+?)\*(?!\*)/g, '<em>$1</em>')
    .replace(/_(.+?)_/g, '<em>$1</em>')
}

// ── Shared constants ──────────────────────────────────────────────────────────

const STATUS_WA = {
  idle:      { label: 'Aguardando',    cls: 'bg-gray-100 text-gray-500' },
  searching: { label: 'Buscando carga', cls: 'bg-amber-100 text-amber-700' },
  matched:   { label: 'Carga aceita',   cls: 'bg-green-100 text-green-700' },
  in_trip:   { label: 'Em viagem',      cls: 'bg-blue-100 text-blue-700' },
  delivered: { label: 'Entregue',       cls: 'bg-purple-100 text-purple-700' },
}

const STATUS_OPS = {
  idle:      { label: '💤 Aguardando',    border: 'border-gray-700',  badge: 'bg-gray-800 text-gray-400' },
  searching: { label: '🔍 Buscando',      border: 'border-amber-500', badge: 'bg-amber-950 text-amber-400' },
  matched:   { label: '✅ Carga aceita',  border: 'border-green-500', badge: 'bg-green-950 text-green-400' },
  in_trip:   { label: '🚛 Em viagem',     border: 'border-blue-500',  badge: 'bg-blue-950 text-blue-400' },
  delivered: { label: '🏁 Entregue',      border: 'border-purple-500', badge: 'bg-purple-950 text-purple-400' },
}

const SUGGESTIONS = [
  { short: 'Aceitar carga nº 1', full: 'Quero a carga número 1' },
  { short: 'Iniciei o trajeto',  full: 'Saí para fazer a coleta, iniciei o trajeto agora' },
  { short: 'Entrega concluída',  full: 'Cheguei no destino, entrega concluída!' },
]

function getInitials(name = '') {
  return name.split(' ').slice(0, 2).map(p => p[0]).join('').toUpperCase()
}

// ── Left panel: WhatsApp simulator ───────────────────────────────────────────

function WhatsAppPanel({ drivers, convs, setConvs, isSendingRef }) {
  const [selectedId, setSelectedId] = useState(null)
  const [message, setMessage]       = useState('')
  const [loading, setLoading]       = useState(false)
  const [recording, setRecording]     = useState(false)
  const [audioBlob, setAudioBlob]     = useState(null)
  const [recordingTime, setRecordingTime] = useState(0)
  const mediaRecorderRef = useRef(null)
  const chunksRef        = useRef([])
  const streamRef        = useRef(null)
  const timerRef         = useRef(null)
  const messagesEndRef     = useRef(null)
  const scrollContainerRef = useRef(null)
  const isAtBottomRef      = useRef(true)
  const prevSelectedIdRef  = useRef(null)
  const [showScrollBtn, setShowScrollBtn] = useState(false)

  useEffect(() => {
    if (!selectedId && drivers.length > 0) setSelectedId(drivers[0].id)
  }, [drivers, selectedId])

  const handleScroll = () => {
    const el = scrollContainerRef.current
    if (!el) return
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 80
    isAtBottomRef.current = atBottom
    setShowScrollBtn(!atBottom)
  }

  const scrollToBottom = (behavior = 'smooth') => {
    messagesEndRef.current?.scrollIntoView({ behavior })
    isAtBottomRef.current = true
    setShowScrollBtn(false)
  }

  useEffect(() => {
    const switched = prevSelectedIdRef.current !== selectedId
    prevSelectedIdRef.current = selectedId
    if (switched) {
      scrollToBottom('instant')
    } else if (isAtBottomRef.current) {
      scrollToBottom('smooth')
    }
  }, [convs, selectedId])

  const driver = drivers.find(d => d.id === selectedId)
  const conv   = selectedId ? (convs[selectedId] || { messages: [], status: 'idle', context: {} }) : null

  const sendText = async (text = message) => {
    const trimmed = text.trim()
    if (!trimmed || !selectedId || loading) return
    setMessage('')
    setLoading(true)
    if (isSendingRef) isSendingRef.current = true

    const now = new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })
    setConvs(prev => {
      const conv = prev[selectedId] || { messages: [], status: 'idle', context: {} }
      return {
        ...prev,
        [selectedId]: {
          ...conv,
          messages: [...(conv.messages || []), { role: 'driver', content: trimmed, type: 'text', timestamp: now }],
        },
      }
    })
    scrollToBottom()

    try {
      const r = await api.post('/message', { driver_id: selectedId, message: trimmed })
      const replyTs = new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })
      setConvs(prev => {
        const current = prev[selectedId] || { messages: [], status: 'idle', context: {} }
        const dbState = r.data.state || {}
        return {
          ...prev,
          [selectedId]: {
            ...dbState,
            messages: [
              ...(current.messages || []),
              { role: 'assistant', content: r.data.response, type: 'text', timestamp: replyTs },
            ],
          },
        }
      })
    } catch (e) {
      alert(e.response?.data?.detail || 'Erro ao processar mensagem.')
    } finally {
      if (isSendingRef) isSendingRef.current = false
      setLoading(false)
    }
  }

  const fmtTime = (s) => `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`

  const startRecording = async () => {
    try {
      chunksRef.current = []
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
      streamRef.current = stream
      const mimeType = MediaRecorder.isTypeSupported('audio/webm;codecs=opus') ? 'audio/webm;codecs=opus' : 'audio/webm'
      const mr = new MediaRecorder(stream, { mimeType })
      mr.ondataavailable = e => { if (e.data.size > 0) chunksRef.current.push(e.data) }
      mr.onstop = () => {
        if (chunksRef.current.length > 0) setAudioBlob(new Blob(chunksRef.current, { type: mimeType }))
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

  const cancelRecording = () => {
    clearInterval(timerRef.current)
    chunksRef.current = []
    if (mediaRecorderRef.current?.state === 'recording') mediaRecorderRef.current.stop()
    streamRef.current?.getTracks().forEach(t => t.stop())
    streamRef.current = null
    setRecording(false)
    setAudioBlob(null)
  }

  // Converte qualquer formato de áudio para WAV (16 kHz mono PCM) via Web Audio API.
  // O Whisper no Databricks exige áudio decodificável por librosa — WAV PCM é universalmente compatível.
  const blobToWav = async (blob) => {
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

  const sendAudio = async () => {
    if (!audioBlob || !selectedId || loading) return
    setLoading(true)
    if (isSendingRef) isSendingRef.current = true

    const audioUrl = URL.createObjectURL(audioBlob)
    const now = new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })

    // Optimistic: mostra player imediatamente (content vazio = ainda transcrevendo)
    setConvs(prev => {
      const current = prev[selectedId] || { messages: [], status: 'idle', context: {} }
      return { ...prev, [selectedId]: { ...current,
        messages: [...(current.messages || []), { role: 'driver', content: '', type: 'audio', audioUrl, timestamp: now }],
      }}
    })

    // Converte WebM → WAV antes de enviar (Whisper precisa de PCM)
    let uploadBlob = audioBlob
    try { uploadBlob = await blobToWav(audioBlob) } catch (_) { /* usa original se falhar */ }
    setAudioBlob(null)

    const form = new FormData()
    form.append('file', uploadBlob, 'audio.wav')
    try {
      const r = await api.post(`/audio/${selectedId}`, form, { headers: { 'Content-Type': 'multipart/form-data' } })
      setConvs(prev => {
        const dbState = r.data.state || {}
        // Usa o estado do DB como fonte de verdade (já contém a mensagem de áudio + resposta).
        // Apenas restaura audioUrl a partir de audio_url para que o player funcione.
        const messages = (dbState.messages || []).map(m =>
          m.type === 'audio' ? { ...m, audioUrl: m.audio_url || undefined } : m
        )
        return { ...prev, [selectedId]: { ...dbState, messages } }
      })
    } catch (e) {
      alert(e.response?.data?.detail || 'Erro na transcrição de áudio.')
    } finally {
      if (isSendingRef) isSendingRef.current = false
      setLoading(false)
    }
  }

  const resetConv = async (id) => {
    await api.delete(`/state/${id}`)
    setConvs(prev => ({ ...prev, [id]: { messages: [], status: 'idle', context: {} } }))
  }

  return (
    <div className="flex h-full" style={{ background: '#e5ddd5' }}>

      {/* Contacts sidebar */}
      <div className="w-52 min-w-[180px] bg-white flex flex-col border-r border-gray-200 flex-shrink-0">
        <div className="bg-motz px-3 py-2.5 flex items-center justify-between">
          <span className="text-white font-semibold text-xs tracking-wide">Motoristas</span>
          <button
            onClick={async () => { await api.delete('/state'); setConvs({}) }}
            className="text-orange-200 hover:text-white text-[10px] transition-colors"
          >
            Limpar
          </button>
        </div>
        <div className="flex-1 overflow-y-auto scrollbar-thin">
          {drivers.map(d => {
            const s    = convs[d.id]?.status || 'idle'
            const cfg  = STATUS_WA[s] || STATUS_WA.idle
            const last = convs[d.id]?.messages?.slice(-1)[0]?.content || ''
            const active = d.id === selectedId
            return (
              <button
                key={d.id}
                onClick={() => setSelectedId(d.id)}
                className={`w-full text-left flex items-center gap-2 px-3 py-2.5 border-b border-gray-50 transition-colors ${active ? 'bg-motz-bg' : 'hover:bg-gray-50'}`}
              >
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-white font-bold text-[11px] flex-shrink-0 ${active ? 'bg-motz' : 'bg-gray-300'}`}>
                  {getInitials(d.nome)}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-baseline gap-1 min-w-0">
                    <span className="font-semibold text-[11px] text-gray-900 truncate">{d.nome.split(' ')[0]}</span>
                    <span className="text-[9px] text-gray-400 flex-shrink-0">{d.localizacao_estado}</span>
                  </div>
                  <span className={`inline-block text-[9px] px-1.5 py-0.5 rounded-full font-semibold ${cfg.cls}`}>{cfg.label}</span>
                  {last && <p className="text-[9px] text-gray-400 truncate mt-0.5">{last}</p>}
                </div>
              </button>
            )
          })}
        </div>
      </div>

      {/* Chat area */}
      {driver && conv ? (
        <div className="flex-1 flex flex-col min-w-0 relative">

          {/* chat header */}
          <div className="bg-motz px-4 py-2.5 flex items-center gap-2.5 shadow-sm flex-shrink-0">
            <div className="w-8 h-8 rounded-full bg-orange-400 flex items-center justify-center text-white font-bold text-[11px] flex-shrink-0">
              {getInitials(driver.nome)}
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-white font-semibold text-sm">{driver.nome}</div>
              <div className="text-orange-100 text-[10px] truncate">
                {driver.veiculo.composicao} {driver.veiculo.caracteristica} · {driver.localizacao_atual}/{driver.localizacao_estado}
              </div>
            </div>
            <button onClick={() => resetConv(driver.id)} className="text-orange-200 hover:text-white transition-colors" title="Limpar conversa">
              <Trash2 size={14} />
            </button>
          </div>

          {/* accepted load banner */}
          {conv.context?.carga_aceita && (
            <div className="bg-green-50 border-b border-green-200 px-4 py-1.5 flex items-center gap-2 text-xs text-green-800 flex-shrink-0">
              <span className="font-semibold">Carga aceita:</span>
              <span className="truncate">
                {conv.context.carga_aceita.tipo_carga} ·{' '}
                {conv.context.carga_aceita.origem_estado} → {conv.context.carga_aceita.destino_estado} ·{' '}
                <strong>R$ {conv.context.carga_aceita.valor_frete?.toLocaleString('pt-BR')}</strong>
              </span>
            </div>
          )}

          {/* messages */}
          <div
            ref={scrollContainerRef}
            onScroll={handleScroll}
            className="flex-1 overflow-y-auto px-4 py-3 flex flex-col gap-1.5 scrollbar-thin"
            style={{ backgroundImage: "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='400' height='400'%3E%3Crect fill='%23ddd6cb' width='400' height='400'/%3E%3C/svg%3E\")" }}
          >
            {conv.messages?.length === 0 && (
              <div className="flex flex-col items-center justify-center h-full text-gray-400 gap-2">
                <span className="text-2xl">💬</span>
                <p className="text-xs text-gray-500">Use as sugestões abaixo para iniciar</p>
              </div>
            )}
            {conv.messages?.map((msg, i) => {
              const isDriver = msg.role === 'driver'
              const audioSrc = msg.audioUrl || msg.audio_url || null
              const isAudio  = msg.type === 'audio' && audioSrc
              return (
                <div key={i} className={`flex ${isDriver ? 'justify-end' : 'justify-start'}`}>
                  <div className={`max-w-[80%] px-3 py-2 rounded-2xl text-xs shadow-sm ${isDriver ? 'bg-motz-bg-dark text-gray-800 rounded-br-sm' : 'bg-white text-gray-800 rounded-bl-sm'}`}>
                    {isAudio ? (
                      <>
                        <audio controls src={audioSrc} className="w-44 h-8 mb-1.5" />
                        <p className={`text-[11px] leading-relaxed ${msg.content ? 'text-gray-700' : 'text-gray-400 italic'}`}>
                          {msg.content || 'Transcrevendo…'}
                        </p>
                      </>
                    ) : (
                      <p className="whitespace-pre-wrap leading-relaxed" dangerouslySetInnerHTML={{ __html: renderMarkdown(msg.content) }} />
                    )}
                    <p className={`text-[9px] mt-0.5 text-right ${isDriver ? 'text-orange-400' : 'text-gray-300'}`}>{msg.timestamp}</p>
                  </div>
                </div>
              )
            })}
            {loading && (
              <div className="flex justify-start">
                <div className="bg-white rounded-2xl rounded-bl-sm px-3 py-2.5 shadow-sm">
                  <div className="flex gap-1 items-center">
                    {[0, 150, 300].map(d => (
                      <span key={d} className="w-1.5 h-1.5 bg-gray-300 rounded-full animate-bounce" style={{ animationDelay: `${d}ms` }} />
                    ))}
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>

          {/* scroll-to-bottom button */}
          {showScrollBtn && (
            <button
              onClick={() => scrollToBottom()}
              className="absolute bottom-20 right-4 w-8 h-8 bg-white rounded-full shadow-lg border border-gray-200 flex items-center justify-center text-gray-500 hover:text-motz hover:border-motz transition-all z-10"
              title="Ir para as mensagens mais recentes"
            >
              <ChevronDown size={15} />
            </button>
          )}

          {/* suggestions */}
          <div className="bg-white border-t border-gray-100 px-3 py-1.5 flex gap-1.5 overflow-x-auto scrollbar-thin flex-shrink-0">
            <span className="flex items-center gap-1 text-[10px] text-gray-400 flex-shrink-0">
              <Zap size={10} className="text-motz" /> Sugestões:
            </span>
            {SUGGESTIONS.map((s, i) => (
              <button
                key={i}
                onClick={() => sendText(s.full)}
                disabled={loading}
                className="flex-shrink-0 text-[10px] bg-motz-bg hover:bg-motz hover:text-white text-motz border border-orange-200 rounded-full px-2.5 py-1 transition-all disabled:opacity-40 font-medium"
              >
                {s.short}
              </button>
            ))}
          </div>

          {/* input */}
          <div className="bg-[#f0f0f0] px-3 py-2 flex items-center gap-2 border-t border-gray-200 flex-shrink-0">
            {recording ? (
              /* ── Gravando ── */
              <>
                <button onClick={cancelRecording} className="w-8 h-8 rounded-full bg-gray-300 hover:bg-gray-400 flex items-center justify-center text-gray-600 flex-shrink-0 transition-all" title="Cancelar">
                  <span className="text-xs font-bold">✕</span>
                </button>
                <div className="flex-1 flex items-center gap-2 bg-white rounded-full px-3 py-1.5">
                  <span className="relative flex h-2.5 w-2.5 flex-shrink-0">
                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75" />
                    <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-red-500" />
                  </span>
                  <span className="text-red-500 text-[11px] font-semibold">Gravando</span>
                  <span className="text-gray-500 text-[11px] tabular-nums ml-1">{fmtTime(recordingTime)}</span>
                  <div className="flex items-center gap-0.5 ml-auto">
                    {[3,5,4,6,3,5,4,3,6,5].map((h, i) => (
                      <div
                        key={i}
                        className="w-0.5 bg-red-400 rounded-full"
                        style={{ height: `${h * 2}px`, animationDelay: `${i * 80}ms`, animation: 'pulse 0.6s ease-in-out infinite alternate' }}
                      />
                    ))}
                  </div>
                </div>
                <button onClick={stopRecording} className="w-8 h-8 bg-red-500 hover:bg-red-600 rounded-full flex items-center justify-center text-white flex-shrink-0 transition-all" title="Parar gravação">
                  <div className="w-3 h-3 bg-white rounded-sm" />
                </button>
              </>
            ) : audioBlob ? (
              /* ── Áudio pronto para enviar ── */
              <>
                <button onClick={() => setAudioBlob(null)} className="w-8 h-8 rounded-full bg-gray-300 hover:bg-gray-400 flex items-center justify-center text-gray-600 flex-shrink-0 transition-all" title="Descartar">
                  <span className="text-xs font-bold">✕</span>
                </button>
                <div className="flex-1 flex items-center gap-2 bg-white rounded-full px-3 py-1.5">
                  <span className="text-[18px]">🎙️</span>
                  <span className="text-gray-700 text-[11px] font-medium">Áudio pronto para enviar</span>
                  <span className="text-gray-400 text-[10px] ml-auto">{fmtTime(recordingTime)}</span>
                </div>
                <button
                  onClick={sendAudio}
                  disabled={loading}
                  className="w-8 h-8 bg-motz hover:bg-motz-dark disabled:bg-gray-300 rounded-full flex items-center justify-center text-white transition-all flex-shrink-0"
                >
                  {loading ? <div className="w-3 h-3 border-2 border-white/40 border-t-white rounded-full animate-spin" /> : <Send size={13} />}
                </button>
              </>
            ) : (
              /* ── Normal ── */
              <>
                <button
                  onClick={startRecording}
                  disabled={loading}
                  className="w-8 h-8 rounded-full flex items-center justify-center bg-gray-200 hover:bg-gray-300 text-gray-600 transition-all flex-shrink-0 disabled:opacity-40"
                  title="Gravar áudio"
                >
                  <Mic size={14} />
                </button>
                <input
                  type="text"
                  value={message}
                  onChange={e => setMessage(e.target.value)}
                  onKeyDown={e => e.key === 'Enter' && !e.shiftKey && sendText()}
                  placeholder="Mensagem do motorista..."
                  className="flex-1 bg-white rounded-full px-3 py-2 text-xs outline-none focus:ring-2 focus:ring-motz/30"
                  disabled={loading}
                />
                <button
                  onClick={() => sendText()}
                  disabled={loading || !message.trim()}
                  className="w-8 h-8 bg-motz hover:bg-motz-dark disabled:bg-gray-300 rounded-full flex items-center justify-center text-white transition-all flex-shrink-0"
                >
                  {loading ? <div className="w-3 h-3 border-2 border-white/40 border-t-white rounded-full animate-spin" /> : <Send size={13} />}
                </button>
              </>
            )}
          </div>
        </div>
      ) : (
        <div className="flex-1 flex items-center justify-center text-gray-400 text-xs">
          Selecione um motorista
        </div>
      )}
    </div>
  )
}

// ── Right panel: Live operations ──────────────────────────────────────────────

function OpsPanel({ drivers, convs, lastUpdated, autoRefresh, setAutoRefresh, available }) {
  const convList = Object.values(convs)
  const kpis = {
    ativas:   convList.filter(c => c.status !== 'idle').length,
    buscando: convList.filter(c => c.status === 'searching').length,
    aceitas:  convList.filter(c => c.status === 'matched').length,
    emViagem: convList.filter(c => c.status === 'in_trip').length,
    valor:    convList
      .filter(c => ['matched', 'in_trip', 'delivered'].includes(c.status))
      .reduce((acc, c) => acc + (c.context?.carga_aceita?.valor_frete || 0), 0),
  }

  const feed = drivers
    .flatMap(d => (convs[d.id]?.messages || []).map(m => ({ ...m, driverName: d.nome.split(' ')[0] })))
    .sort((a, b) => (a.timestamp || '').localeCompare(b.timestamp || ''))
    .slice(-30)
    .reverse()

  return (
    <div className="h-full bg-gray-950 text-white flex flex-col overflow-hidden">

      {/* Header */}
      <div className="flex items-center justify-between px-5 pt-4 pb-3 flex-shrink-0">
        <div className="flex items-center gap-2">
          <span className="relative flex h-2.5 w-2.5">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
            <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-green-500" />
          </span>
          <h2 className="text-base font-extrabold text-white tracking-tight">Central de Operações</h2>
          {lastUpdated && <span className="text-[10px] text-gray-600 ml-1">· {lastUpdated}</span>}
        </div>
        <label className="flex items-center gap-1.5 text-[11px] text-gray-500 cursor-pointer select-none">
          <input type="checkbox" checked={autoRefresh} onChange={e => setAutoRefresh(e.target.checked)} className="accent-motz" />
          <RefreshCw size={11} className={autoRefresh ? 'text-green-400 animate-spin' : 'text-gray-600'} style={{ animationDuration: '3s' }} />
        </label>
      </div>

      {/* KPIs */}
      <div className="flex items-center gap-2 px-5 mb-2 flex-shrink-0">
        <span className="text-[9px] font-bold text-gray-500 uppercase tracking-widest">Hoje</span>
        <span className="text-[10px] bg-gray-800 text-gray-400 px-2 py-0.5 rounded-full border border-gray-700 tabular-nums">
          {new Date().toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' })}
        </span>
      </div>
      <div className="grid grid-cols-5 gap-2 px-5 mb-4 flex-shrink-0">
        {[
          { value: kpis.ativas,  label: 'Ativas',    color: 'text-motz' },
          { value: kpis.buscando, label: 'Buscando',  color: 'text-amber-400' },
          { value: kpis.aceitas,  label: 'Aceitas',   color: 'text-green-400' },
          { value: kpis.emViagem, label: 'Em viagem', color: 'text-blue-400' },
          { value: `R$${(kpis.valor/1000).toFixed(0)}k`, label: 'Negociado', color: 'text-motz' },
        ].map(k => (
          <div key={k.label} className="bg-gray-900 rounded-xl px-3 py-2.5 border border-gray-800">
            <div className={`text-xl font-extrabold ${k.color} tabular-nums`}>{k.value}</div>
            <div className="text-gray-500 text-[9px] uppercase tracking-widest mt-0.5">{k.label}</div>
          </div>
        ))}
      </div>

      {/* Cargas disponíveis — compacto */}
      {available && (
        <div className="mx-5 mb-3 bg-gray-900 border border-gray-800 rounded-xl px-4 py-3 flex-shrink-0">
          <div className="flex items-center gap-3 mb-2.5">
            <span className="text-[10px] font-bold text-gray-500 uppercase tracking-widest">Cargas disponíveis</span>
            <span className="text-lg font-extrabold text-motz tabular-nums">{available.totais.total}</span>
            {available.totais.atrasadas > 0 && (
              <span className="text-[9px] bg-red-950 text-red-400 px-1.5 py-0.5 rounded-full font-bold ml-auto">
                {available.totais.atrasadas} atrasadas
              </span>
            )}
          </div>
          <div className="grid grid-cols-4 gap-2 mb-2.5">
            {[
              { label: 'Hoje',      value: available.totais.hoje,            color: 'text-red-400',    bar: 'bg-red-500' },
              { label: '3 dias',    value: available.totais.proximos_3_dias, color: 'text-amber-400',  bar: 'bg-amber-500' },
              { label: '7 dias',    value: available.totais.proxima_semana,  color: 'text-yellow-300', bar: 'bg-yellow-400' },
              { label: '30 dias',   value: available.totais.proximo_mes,     color: 'text-green-400',  bar: 'bg-green-500' },
            ].map(p => {
              const pct = available.totais.total > 0 ? Math.round((p.value / available.totais.total) * 100) : 0
              return (
                <div key={p.label} className="bg-gray-800 rounded-lg px-2.5 py-2">
                  <div className={`text-base font-extrabold tabular-nums ${p.color}`}>{p.value}</div>
                  <div className="text-gray-600 text-[9px] uppercase tracking-widest mb-1">{p.label}</div>
                  <div className="h-0.5 bg-gray-700 rounded-full overflow-hidden">
                    <div className={`h-full rounded-full ${p.bar}`} style={{ width: `${pct}%` }} />
                  </div>
                </div>
              )
            })}
          </div>
          <div className="flex flex-wrap gap-1.5">
            {available.por_tipo.map(t => (
              <span key={t.tipo_carga} className="text-[9px] bg-gray-800 text-gray-400 rounded px-2 py-0.5">
                {t.tipo_carga} <span className="text-motz font-bold">{t.quantidade}</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Body: driver cards + activity feed */}
      <div className="flex flex-1 gap-4 px-5 pb-5 min-h-0">

        {/* Driver cards */}
        <div className="flex-1 overflow-y-auto scrollbar-thin min-w-0 space-y-2">
          <h3 className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2">Motoristas ativos</h3>
          {drivers.filter(d => (convs[d.id]?.status || 'idle') !== 'idle').length === 0 && drivers.length > 0 && (
            <p className="text-gray-700 text-[11px] text-center py-6">Nenhum motorista ativo.</p>
          )}
          {drivers.filter(d => (convs[d.id]?.status || 'idle') !== 'idle').map(d => {
            const s   = convs[d.id]?.status || 'idle'
            const cfg = STATUS_OPS[s] || STATUS_OPS.idle
            const ctx = convs[d.id]?.context || {}
            const carga = ctx.carga_aceita
            const lastMsg = convs[d.id]?.messages?.slice(-1)[0]?.content
            return (
              <div key={d.id} className={`bg-gray-900 rounded-xl p-3 border-l-2 ${cfg.border} border border-gray-800 transition-all`}>
                <div className="flex items-center gap-2">
                  <div className="w-8 h-8 rounded-full bg-gray-800 border border-gray-700 flex items-center justify-center text-motz font-bold text-[10px] flex-shrink-0">
                    {getInitials(d.nome)}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <span className="font-semibold text-white text-xs">{d.nome}</span>
                      <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold ${cfg.badge}`}>{cfg.label}</span>
                    </div>
                    <p className="text-gray-600 text-[9px] mt-0.5 truncate">
                      {d.veiculo.composicao} {d.veiculo.caracteristica} · {d.localizacao_atual}/{d.localizacao_estado}
                    </p>
                  </div>
                </div>
                {carga && (
                  <div className="mt-2 bg-gray-800 rounded-lg px-2.5 py-1.5 text-[10px] text-gray-300 flex flex-wrap gap-x-2 gap-y-0.5">
                    <span className="font-medium text-white">{carga.tipo_carga}</span>
                    <span>{carga.origem_estado} → {carga.destino_estado}</span>
                    <span className="text-motz font-semibold">R$ {carga.valor_frete?.toLocaleString('pt-BR')}</span>
                  </div>
                )}
                {lastMsg && !carga && (
                  <p className="mt-1 text-[9px] text-gray-700 italic truncate">"{lastMsg}"</p>
                )}
              </div>
            )
          })}
        </div>

        {/* Activity feed */}
        <div className="w-52 flex-shrink-0 flex flex-col min-h-0">
          <h3 className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2">Feed ao vivo</h3>
          <div className="flex-1 bg-gray-900 rounded-xl border border-gray-800 overflow-y-auto scrollbar-thin divide-y divide-gray-800">
            {feed.length === 0 && (
              <div className="text-center text-gray-700 py-8 text-[10px] px-3">
                Nenhuma atividade ainda.<br />
                <span className="text-gray-800">Inicie uma conversa ao lado.</span>
              </div>
            )}
            {feed.map((msg, i) => (
              <div key={i} className="px-3 py-2 hover:bg-gray-800/50 transition-colors">
                <div className="flex items-center gap-1.5 mb-0.5">
                  <span className="text-motz text-[9px] font-bold">{msg.timestamp}</span>
                  <span className="text-gray-600 text-[9px]">{msg.role === 'driver' ? '👤' : '🤖'} {msg.driverName}</span>
                </div>
                <p className="text-gray-400 text-[9px] leading-relaxed line-clamp-2">
                  {msg.type === 'audio' ? (msg.content ? `🎙️ ${msg.content}` : '🎙️ Áudio') : msg.content}
                </p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Main Demo page ────────────────────────────────────────────────────────────

export default function Demo() {
  const [drivers, setDrivers]         = useState([])
  const [convs, setConvs]             = useState({})
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [available, setAvailable]     = useState(null)
  const isSendingRef                  = useRef(false)

  useEffect(() => {
    api.get('/drivers').then(r => setDrivers(r.data))
    api.get('/loads/available').then(r => setAvailable(r.data))
  }, [])

  useEffect(() => {
    const load = () => {
      if (isSendingRef.current) return
      api.get('/state').then(r => {
        if (isSendingRef.current) return
        const serverConvs = r.data.conversations || {}
        // Preserva audioUrl das mensagens locais ao mergear com estado do servidor
        // (blob URLs só existem no browser e não são persistidas no DB)
        setConvs(prev => {
          const merged = {}
          for (const [dId, serverConv] of Object.entries(serverConvs)) {
            const existing = prev[dId]
            if (!existing?.messages?.length) { merged[dId] = serverConv; continue }
            const audioUrls = {}
            existing.messages.forEach(m => { if (m.audioUrl && m.timestamp) audioUrls[m.timestamp] = m.audioUrl })
            const messages = (serverConv.messages || []).map(m => {
              if (m.type !== 'audio') return m
              // Prefer local blob URL (in-progress) → server persisted URL → field from DB
              const localUrl = m.timestamp && audioUrls[m.timestamp]
              return { ...m, audioUrl: localUrl || m.audio_url || undefined }
            })
            merged[dId] = { ...serverConv, messages }
          }
          return merged
        })
        setLastUpdated(new Date().toLocaleTimeString('pt-BR'))
      })
    }
    load()
    if (!autoRefresh) return
    const id = setInterval(load, 3000)
    return () => clearInterval(id)
  }, [autoRefresh])

  return (
    <div className="flex h-[calc(100vh-64px)] overflow-hidden">
      {/* Left: WhatsApp */}
      <div className="w-1/2 border-r border-gray-300 flex flex-col overflow-hidden flex-shrink-0">
        <WhatsAppPanel drivers={drivers} convs={convs} setConvs={setConvs} isSendingRef={isSendingRef} />
      </div>

      {/* Right: Ops Center */}
      <div className="w-1/2 overflow-hidden flex-shrink-0">
        <OpsPanel
          drivers={drivers}
          convs={convs}
          lastUpdated={lastUpdated}
          autoRefresh={autoRefresh}
          setAutoRefresh={setAutoRefresh}
          available={available}
        />
      </div>
    </div>
  )
}

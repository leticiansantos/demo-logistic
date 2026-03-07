import { useState, useEffect, useRef } from 'react'
import { Send, Mic, MicOff, Trash2, Zap } from 'lucide-react'
import api from '../api/client'

const STATUS = {
  idle:      { label: 'Aguardando',    cls: 'bg-gray-100 text-gray-500' },
  searching: { label: 'Buscando carga', cls: 'bg-amber-100 text-amber-700' },
  matched:   { label: 'Carga aceita',   cls: 'bg-green-100 text-green-700' },
  in_trip:   { label: 'Em viagem',      cls: 'bg-blue-100 text-blue-700' },
  delivered: { label: 'Entregue',       cls: 'bg-purple-100 text-purple-700' },
}

const SUGGESTIONS = [
  { short: 'Buscar carga SP→TO', full: 'Olá! Estou em São Paulo e quero ir para Tocantins, tem carga disponível para minha carreta?' },
  { short: 'Aceitar carga nº 1', full: 'Quero a carga número 1' },
  { short: 'Iniciei o trajeto',  full: 'Saí para fazer a coleta, iniciei o trajeto agora' },
  { short: 'Entrega concluída',  full: 'Cheguei no destino, entrega concluída!' },
]

function getInitials(name = '') {
  return name.split(' ').slice(0, 2).map(p => p[0]).join('').toUpperCase()
}

export default function WhatsApp() {
  const [drivers, setDrivers]       = useState([])
  const [selectedId, setSelectedId] = useState(null)
  const [convs, setConvs]           = useState({})
  const [message, setMessage]       = useState('')
  const [loading, setLoading]       = useState(false)
  const [recording, setRecording]   = useState(false)
  const [audioBlob, setAudioBlob]   = useState(null)
  const mediaRecorderRef = useRef(null)
  const chunksRef        = useRef([])
  const messagesEndRef   = useRef(null)

  // Load drivers once
  useEffect(() => {
    api.get('/drivers').then(r => {
      setDrivers(r.data)
      if (r.data.length > 0) setSelectedId(r.data[0].id)
    })
  }, [])

  // Poll state
  useEffect(() => {
    const load = () => api.get('/state').then(r => setConvs(r.data.conversations || {}))
    load()
    const id = setInterval(load, 3000)
    return () => clearInterval(id)
  }, [])

  // Scroll to bottom on new message
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [convs, selectedId])

  const driver = drivers.find(d => d.id === selectedId)
  const conv   = selectedId ? (convs[selectedId] || { messages: [], status: 'idle', context: {} }) : null

  // ── Send text ──────────────────────────────────────────────────────────────
  const sendText = async (text = message) => {
    if (!text.trim() || !selectedId || loading) return
    setLoading(true)
    try {
      const r = await api.post('/message', { driver_id: selectedId, message: text.trim() })
      setConvs(prev => ({ ...prev, [selectedId]: r.data.state }))
      setMessage('')
    } catch (e) {
      alert(e.response?.data?.detail || 'Erro ao processar mensagem.')
    } finally {
      setLoading(false)
    }
  }

  // ── Audio recording ────────────────────────────────────────────────────────
  const startRecording = async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
      chunksRef.current = []
      const mr = new MediaRecorder(stream)
      mr.ondataavailable = e => chunksRef.current.push(e.data)
      mr.onstop = () => {
        const blob = new Blob(chunksRef.current, { type: 'audio/webm' })
        setAudioBlob(blob)
        stream.getTracks().forEach(t => t.stop())
      }
      mr.start()
      mediaRecorderRef.current = mr
      setRecording(true)
    } catch {
      alert('Permissão de microfone negada.')
    }
  }

  const stopRecording = () => {
    mediaRecorderRef.current?.stop()
    setRecording(false)
  }

  const sendAudio = async () => {
    if (!audioBlob || !selectedId || loading) return
    setLoading(true)
    const formData = new FormData()
    formData.append('file', audioBlob, 'audio.webm')
    try {
      const r = await api.post(`/audio/${selectedId}`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      })
      setConvs(prev => ({ ...prev, [selectedId]: r.data.state }))
      setAudioBlob(null)
    } catch (e) {
      alert(e.response?.data?.detail || 'Erro na transcrição de áudio.')
    } finally {
      setLoading(false)
    }
  }

  const resetConv = async (id) => {
    await api.delete(`/state/${id}`)
    setConvs(prev => ({ ...prev, [id]: { messages: [], status: 'idle', context: {} } }))
  }

  const resetAll = async () => {
    await api.delete('/state')
    setConvs({})
  }

  return (
    <div className="flex h-[calc(100vh-64px)]" style={{ background: '#e5ddd5' }}>

      {/* ── Left: contacts ───────────────────────────────────────────────── */}
      <div className="w-80 min-w-[260px] bg-white flex flex-col border-r border-gray-200">
        {/* header */}
        <div className="bg-motz px-5 py-3.5 flex items-center justify-between">
          <span className="text-white font-semibold text-sm tracking-wide">Motoristas</span>
          <button onClick={resetAll} className="text-orange-200 hover:text-white text-xs transition-colors">
            Limpar tudo
          </button>
        </div>

        {/* list */}
        <div className="flex-1 overflow-y-auto scrollbar-thin">
          {drivers.map(d => {
            const s      = convs[d.id]?.status || 'idle'
            const cfg    = STATUS[s] || STATUS.idle
            const lastMsg = convs[d.id]?.messages?.slice(-1)[0]?.content || ''
            const active = d.id === selectedId
            return (
              <button
                key={d.id}
                onClick={() => setSelectedId(d.id)}
                className={`w-full text-left flex items-center gap-3 px-4 py-3.5 border-b border-gray-50 transition-colors ${
                  active ? 'bg-motz-bg' : 'hover:bg-gray-50'
                }`}
              >
                <div className={`w-11 h-11 rounded-full flex items-center justify-center text-white font-bold text-sm flex-shrink-0 transition-colors ${
                  active ? 'bg-motz' : 'bg-gray-300'
                }`}>
                  {getInitials(d.nome)}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-baseline gap-1.5 min-w-0">
                    <span className="font-semibold text-sm text-gray-900 truncate">{d.nome}</span>
                    <span className="text-[10px] text-gray-400 flex-shrink-0 whitespace-nowrap">{d.localizacao_atual}/{d.localizacao_estado}</span>
                  </div>
                  <span className={`inline-block text-[10px] px-1.5 py-0.5 rounded-full font-semibold mt-0.5 ${cfg.cls}`}>
                    {cfg.label}
                  </span>
                  {lastMsg && (
                    <p className="text-[11px] text-gray-400 truncate mt-0.5">{lastMsg}</p>
                  )}
                </div>
              </button>
            )
          })}
        </div>
      </div>

      {/* ── Right: chat ──────────────────────────────────────────────────── */}
      {driver && conv ? (
        <div className="flex-1 flex flex-col min-w-0">

          {/* chat header */}
          <div className="bg-motz px-5 py-3 flex items-center gap-3 shadow-sm">
            <div className="w-10 h-10 rounded-full bg-orange-400 flex items-center justify-center text-white font-bold text-sm flex-shrink-0">
              {getInitials(driver.nome)}
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-white font-semibold">{driver.nome}</div>
              <div className="text-orange-100 text-xs truncate">
                {driver.veiculo.composicao} {driver.veiculo.caracteristica} ·{' '}
                {driver.veiculo.modelo} · {driver.localizacao_atual}/{driver.localizacao_estado}
              </div>
            </div>
            <button
              onClick={() => resetConv(driver.id)}
              className="text-orange-200 hover:text-white transition-colors"
              title="Limpar conversa"
            >
              <Trash2 size={16} />
            </button>
          </div>

          {/* accepted load banner */}
          {conv.context?.carga_aceita && (
            <div className="bg-green-50 border-b border-green-200 px-5 py-2 flex items-center gap-2 text-sm text-green-800">
              <span className="font-semibold">✅ Carga aceita:</span>
              <span>
                {conv.context.carga_aceita.tipo_carga} ·{' '}
                {conv.context.carga_aceita.origem_cidade}/{conv.context.carga_aceita.origem_estado} →{' '}
                {conv.context.carga_aceita.destino_cidade}/{conv.context.carga_aceita.destino_estado} ·{' '}
                <strong>R$ {conv.context.carga_aceita.valor_frete?.toLocaleString('pt-BR')}</strong>
              </span>
            </div>
          )}

          {/* messages area */}
          <div
            className="flex-1 overflow-y-auto px-8 py-5 flex flex-col gap-2 scrollbar-thin"
            style={{
              backgroundImage:
                "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='400' height='400'%3E%3Crect fill='%23ddd6cb' width='400' height='400'/%3E%3C/svg%3E\")",
            }}
          >
            {conv.messages?.length === 0 && (
              <div className="flex flex-col items-center justify-center h-full text-gray-400 gap-3">
                <div className="w-16 h-16 rounded-full bg-motz-bg flex items-center justify-center">
                  <span className="text-2xl">💬</span>
                </div>
                <p className="text-sm font-medium text-gray-500">Nenhuma mensagem ainda</p>
                <p className="text-xs text-gray-400">Use as sugestões abaixo para iniciar</p>
              </div>
            )}

            {conv.messages?.map((msg, i) => {
              const isDriver = msg.role === 'driver'
              return (
                <div key={i} className={`flex ${isDriver ? 'justify-end' : 'justify-start'}`}>
                  <div
                    className={`max-w-[68%] px-3.5 py-2.5 rounded-2xl text-sm shadow-sm ${
                      isDriver
                        ? 'bg-motz-bg-dark text-gray-800 rounded-br-sm'
                        : 'bg-white text-gray-800 rounded-bl-sm'
                    }`}
                  >
                    <p className="whitespace-pre-wrap leading-relaxed">{msg.content}</p>
                    <p className={`text-[10px] mt-1 text-right ${isDriver ? 'text-orange-400' : 'text-gray-300'}`}>
                      {msg.timestamp}
                    </p>
                  </div>
                </div>
              )
            })}

            {loading && (
              <div className="flex justify-start">
                <div className="bg-white rounded-2xl rounded-bl-sm px-4 py-3 shadow-sm">
                  <div className="flex gap-1 items-center">
                    <span className="w-1.5 h-1.5 bg-gray-300 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                    <span className="w-1.5 h-1.5 bg-gray-300 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                    <span className="w-1.5 h-1.5 bg-gray-300 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
                  </div>
                </div>
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>

          {/* quick suggestions */}
          <div className="bg-white border-t border-gray-100 px-4 py-2 flex gap-2 overflow-x-auto scrollbar-thin">
            <span className="flex items-center gap-1 text-xs text-gray-400 flex-shrink-0">
              <Zap size={11} className="text-motz" /> Sugestões:
            </span>
            {SUGGESTIONS.map((s, i) => (
              <button
                key={i}
                onClick={() => sendText(s.full)}
                disabled={loading}
                className="flex-shrink-0 text-xs bg-motz-bg hover:bg-motz hover:text-white text-motz border border-orange-200 rounded-full px-3 py-1.5 transition-all disabled:opacity-40 font-medium"
              >
                {s.short}
              </button>
            ))}
          </div>

          {/* input area */}
          <div className="bg-[#f0f0f0] px-4 py-3 flex items-center gap-3 border-t border-gray-200">

            {/* mic button */}
            {audioBlob ? (
              <div className="flex items-center gap-2 flex-1 bg-white rounded-full px-4 py-2 text-sm">
                <span className="text-motz text-xs font-medium">🎙️ Áudio gravado — pronto para enviar</span>
                <button onClick={() => setAudioBlob(null)} className="ml-auto text-gray-400 hover:text-gray-600">✕</button>
              </div>
            ) : (
              <>
                <button
                  onMouseDown={startRecording}
                  onMouseUp={stopRecording}
                  onTouchStart={startRecording}
                  onTouchEnd={stopRecording}
                  className={`w-10 h-10 rounded-full flex items-center justify-center transition-all flex-shrink-0 ${
                    recording ? 'bg-red-500 scale-110 shadow-lg' : 'bg-gray-200 hover:bg-gray-300 text-gray-600'
                  }`}
                  title="Segure para gravar áudio"
                >
                  {recording ? <MicOff size={18} className="text-white" /> : <Mic size={18} />}
                </button>

                <input
                  type="text"
                  value={message}
                  onChange={e => setMessage(e.target.value)}
                  onKeyDown={e => e.key === 'Enter' && !e.shiftKey && sendText()}
                  placeholder="Mensagem do motorista..."
                  className="flex-1 bg-white rounded-full px-4 py-2.5 text-sm outline-none focus:ring-2 focus:ring-motz/30 transition-all"
                  disabled={loading}
                />
              </>
            )}

            {/* send button */}
            <button
              onClick={audioBlob ? sendAudio : () => sendText()}
              disabled={loading || (!message.trim() && !audioBlob)}
              className="w-10 h-10 bg-motz hover:bg-motz-dark disabled:bg-gray-300 rounded-full flex items-center justify-center text-white transition-all shadow-sm flex-shrink-0"
            >
              {loading ? (
                <div className="w-4 h-4 border-2 border-white/40 border-t-white rounded-full animate-spin" />
              ) : (
                <Send size={16} />
              )}
            </button>
          </div>
        </div>

      ) : (
        <div className="flex-1 flex items-center justify-center text-gray-400">
          <p className="text-sm">Selecione um motorista para começar</p>
        </div>
      )}
    </div>
  )
}

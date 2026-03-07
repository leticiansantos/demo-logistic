import { useState, useEffect, useCallback } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { MessageSquare, Radio, BarChart2, Play, Loader2, Presentation } from 'lucide-react'
import api from '../api/client'
import motzLogo from '../assets/motz-logo.svg'

const tabs = [
  { path: '/',          label: 'Simulador WhatsApp', Icon: MessageSquare },
  { path: '/ao-vivo',   label: 'Central Ao Vivo',    Icon: Radio },
  { path: '/dashboard', label: 'Dashboard',           Icon: BarChart2 },
  { path: '/demo',      label: 'Demo',                Icon: Presentation },
]

const STATE_CFG = {
  RUNNING: { dot: 'bg-green-400', label: 'Online',    btn: null },
  STARTING: { dot: 'bg-yellow-400 animate-pulse', label: 'Iniciando…', btn: null },
  STOPPING: { dot: 'bg-yellow-400 animate-pulse', label: 'Parando…',  btn: null },
  STOPPED:  { dot: 'bg-red-400',   label: 'Offline',   btn: 'Iniciar backend' },
  UNKNOWN:  { dot: 'bg-gray-300',  label: '…',          btn: null },
}

function BackendButton() {
  const [status, setStatus] = useState(null)   // null = loading
  const [starting, setStarting] = useState(false)

  const fetchStatus = useCallback(() => {
    api.get('/backend/status')
      .then(r => setStatus(r.data))
      .catch(() => setStatus(null))
  }, [])

  useEffect(() => {
    fetchStatus()
    const id = setInterval(fetchStatus, 15000)
    return () => clearInterval(id)
  }, [fetchStatus])

  // Poll faster while starting
  useEffect(() => {
    if (!status) return
    const wh = status.warehouse?.state
    if (wh === 'STARTING' || wh === 'STOPPING') {
      const id = setInterval(fetchStatus, 3000)
      return () => clearInterval(id)
    }
  }, [status, fetchStatus])

  const handleStart = async () => {
    setStarting(true)
    try {
      await api.post('/backend/start')
      await fetchStatus()
    } finally {
      setStarting(false)
    }
  }

  if (!status) {
    return (
      <div className="flex items-center gap-1.5 text-xs text-gray-400 px-3 py-2">
        <Loader2 size={12} className="animate-spin" />
        <span>Databricks</span>
      </div>
    )
  }

  const whState = status.warehouse?.state || 'UNKNOWN'
  const cfg = STATE_CFG[whState] || STATE_CFG.UNKNOWN
  const isReady = status.ready

  return (
    <div className="flex items-center gap-2">
      {/* Status pill */}
      <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-gray-50 border border-gray-200">
        <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${cfg.dot}`} />
        <span className="text-[11px] font-medium text-gray-600">{cfg.label}</span>
        {isReady && (
          <span className="text-[10px] text-gray-400 ml-0.5">
            · {status.warehouse?.name}
          </span>
        )}
      </div>

      {/* Start button — only when stopped */}
      {cfg.btn && (
        <button
          onClick={handleStart}
          disabled={starting}
          className="flex items-center gap-1.5 text-xs font-medium text-white bg-motz hover:bg-motz-dark disabled:opacity-60 transition-colors px-3 py-1.5 rounded-lg shadow-sm"
        >
          {starting
            ? <Loader2 size={12} className="animate-spin" />
            : <Play size={12} />}
          {starting ? 'Iniciando…' : cfg.btn}
        </button>
      )}
    </div>
  )
}

export default function Navbar() {
  const { pathname } = useLocation()

  return (
    <nav className="bg-white border-b border-gray-100 sticky top-0 z-50 shadow-sm">
      <div className="max-w-screen-xl mx-auto flex items-center h-16 px-6 gap-8">
        {/* Logo */}
        <Link to="/" className="flex items-center gap-2 group">
          <img src={motzLogo} alt="Motz" className="h-7 w-auto" />
          <span className="text-[10px] font-semibold bg-motz-bg text-motz px-1.5 py-0.5 rounded-full uppercase tracking-wide">
            demo
          </span>
        </Link>

        {/* Nav tabs */}
        <div className="flex gap-1 ml-2">
          {tabs.map(({ path, label, Icon }) => {
            const active = pathname === path
            return (
              <Link
                key={path}
                to={path}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                  active
                    ? 'bg-motz text-white shadow-sm'
                    : 'text-gray-500 hover:bg-gray-50 hover:text-gray-800'
                }`}
              >
                <Icon size={15} />
                {label}
              </Link>
            )
          })}
        </div>

        {/* Backend status + start button */}
        <div className="ml-auto">
          <BackendButton />
        </div>
      </div>
    </nav>
  )
}

import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Navbar from './components/Navbar'
import WhatsApp from './pages/WhatsApp'
import AoVivo from './pages/AoVivo'
import Dashboard from './pages/Dashboard'
import Demo from './pages/Demo'

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-50 font-sans">
        <Navbar />
        <Routes>
          <Route path="/" element={<WhatsApp />} />
          <Route path="/ao-vivo" element={<AoVivo />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/demo" element={<Demo />} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}

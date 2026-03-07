import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 60000, // 60s – LLM pode demorar
})

export default api

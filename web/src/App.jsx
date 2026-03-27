import { Route, Routes } from 'react-router-dom'
import { DebugProvider } from './debug/DebugContext.jsx'
import Layout from './Layout.jsx'
import HomePage from './pages/HomePage.jsx'
import CalendarPage from './pages/CalendarPage.jsx'
import PRMakerPage from './pages/PRMakerPage.jsx'
import PRMakerProcessPage from './pages/PRMakerProcessPage.jsx'
import PRMakerCandidatesPage from './pages/PRMakerCandidatesPage.jsx'
import PRMakerRankingPage from './pages/PRMakerRankingPage.jsx'
import PRMakerFinalPage from './pages/PRMakerFinalPage.jsx'
import './App.css'

export default function App() {
  return (
    <DebugProvider>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<HomePage />} />
          <Route path="calendar" element={<CalendarPage />} />
          <Route path="pr-maker" element={<PRMakerPage />} />
          <Route path="pr-maker/process" element={<PRMakerProcessPage />} />
          <Route path="pr-maker/candidates" element={<PRMakerCandidatesPage />} />
          <Route path="pr-maker/ranking" element={<PRMakerRankingPage />} />
          <Route path="pr-maker/final" element={<PRMakerFinalPage />} />
        </Route>
      </Routes>
    </DebugProvider>
  )
}

import { Link, NavLink, Outlet, useLocation } from 'react-router-dom'
import { useDebug } from './debug/DebugContext.jsx'
import DebugLogShelf from './debug/DebugLogShelf.jsx'

export default function Layout() {
  const location = useLocation()
  const { enabled, setEnabled } = useDebug()

  return (
    <div className="page">
      <header className="header-bar">
        <div className="header-inner">
          <Link
            to="/"
            className="brand"
            onClick={(e) => {
              if (location.pathname === '/') {
                e.preventDefault()
                window.scrollTo({ top: 0, behavior: 'smooth' })
              }
            }}
          >
            NorCal Smash
          </Link>
          <div className="header-center">
            <label className="debug-switch" role="switch" aria-checked={enabled} aria-label="Toggle debug mode">
              <input
                type="checkbox"
                className="debug-switch-input"
                checked={enabled}
                onChange={(e) => setEnabled(e.target.checked)}
              />
              <span className="debug-switch-track">
                <span className="debug-switch-thumb" />
              </span>
              <span className="debug-switch-label">Debug</span>
            </label>
          </div>
          <nav className="header-right">
            <NavLink
              to="/calendar"
              className={({ isActive }) =>
                isActive ? 'header-nav-link header-nav-link--active' : 'header-nav-link'
              }
            >
              Calendar
            </NavLink>
            <NavLink
              to="/pr-maker"
              className={({ isActive }) =>
                isActive ? 'header-nav-link header-nav-link--active' : 'header-nav-link'
              }
            >
              PR Maker
            </NavLink>
          </nav>
        </div>
      </header>
      <div className="header-fade" />
      <DebugLogShelf />
      <Outlet />
    </div>
  )
}

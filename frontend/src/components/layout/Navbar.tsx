'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'

const navLinks = [
  { href: '/', label: 'Dashboard' },
  { href: '/standings', label: 'Standings' },
]

export default function Navbar() {
  const pathname = usePathname()

  return (
    <nav className="fixed top-0 left-0 right-0 z-50 bg-[#0a0a0a] border-b border-[#222222]">
      <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Logo */}
          <Link href="/" className="flex items-center gap-3">
            <div className="w-8 h-8 bg-[#E8002D] rounded flex items-center justify-center">
              <span className="text-white font-black text-xs">MGP</span>
            </div>
            <span className="text-white font-bold text-lg tracking-tight">
              MotoGP<span className="text-[#E8002D]">Stats</span>
            </span>
          </Link>

          {/* Nav links */}
          <div className="flex items-center gap-1">
            {navLinks.map((link) => {
              const isActive =
                link.href === '/'
                  ? pathname === '/'
                  : pathname.startsWith(link.href)
              return (
                <Link
                  key={link.href}
                  href={link.href}
                  className={`px-4 py-2 rounded text-sm font-medium transition-colors ${
                    isActive
                      ? 'bg-[#E8002D] text-white'
                      : 'text-[#888888] hover:text-white hover:bg-[#1a1a1a]'
                  }`}
                >
                  {link.label}
                </Link>
              )
            })}
          </div>
        </div>
      </div>
    </nav>
  )
}

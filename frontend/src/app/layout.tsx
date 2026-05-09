import type { Metadata } from 'next'
import './globals.css'
import Navbar from '@/components/layout/Navbar'

export const metadata: Metadata = {
  title: 'MotoGP Stats Dashboard',
  description: 'Professional MotoGP statistics and race analysis',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-[#0a0a0a] text-white min-h-screen">
        <Navbar />
        <main className="pt-16 min-h-screen">
          {children}
        </main>
      </body>
    </html>
  )
}

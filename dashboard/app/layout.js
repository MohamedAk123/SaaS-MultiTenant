import './style.css';

export const metadata = { title: 'SaaS Multi-Tenant | Observatoire', description: 'Activité et indicateurs par tenant' };
export default function Layout({ children }) {
  return <html lang="fr"><body>{children}</body></html>;
}

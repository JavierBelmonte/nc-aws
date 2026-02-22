/* eslint-disable react-hooks/set-state-in-effect */
import { useCallback, useEffect, useRef, useState } from 'react'
import './App.css'
import {
  closeWindow,
  createResource,
  deleteResource,
  executeCopy,
  getResource,
  listProfiles,
  listRegions,
  listResources,
  listServices,
  prepareCopy,
} from './bridge'
import type {
  PanelContext,
  ProfileSummary,
  ResourceDetail,
  ResourceRef,
  ServiceInfo,
} from './types'

type PanelId = 'left' | 'right'

type PanelState = {
  profileName: string
  region: string
  service: string
  services: ServiceInfo[]
  resources: ResourceRef[]
  selectedIndex: number
  selectedIds: Set<string>
  loading: boolean
  error: string | null
}

const DEFAULT_REGIONS = ['us-east-1']
const DEFAULT_SERVICES = [{ id: 's3', label: 'S3' }]

function emptyPanel(): PanelState {
  return {
    profileName: 'default',
    region: 'us-east-1',
    service: 's3',
    services: DEFAULT_SERVICES,
    resources: [],
    selectedIndex: 0,
    selectedIds: new Set<string>(),
    loading: false,
    error: null,
  }
}

function App() {
  const [profiles, setProfiles] = useState<ProfileSummary[]>([])
  const [regionsByProfile, setRegionsByProfile] = useState<Record<string, string[]>>({})
  const [leftPanel, setLeftPanel] = useState<PanelState>(emptyPanel)
  const [rightPanel, setRightPanel] = useState<PanelState>(emptyPanel)
  const [activePanel, setActivePanel] = useState<PanelId>('left')
  const [status, setStatus] = useState('Ready')
  const [detail, setDetail] = useState<ResourceDetail | null>(null)
  const initializedRef = useRef(false)

  const getPanel = useCallback(
    (panelId: PanelId) => (panelId === 'left' ? leftPanel : rightPanel),
    [leftPanel, rightPanel],
  )

  const setPanel = useCallback((panelId: PanelId, updater: (current: PanelState) => PanelState) => {
    if (panelId === 'left') {
      setLeftPanel((current) => updater(current))
    } else {
      setRightPanel((current) => updater(current))
    }
  }, [])

  const loadPanelResources = useCallback(
    async (panelId: PanelId, override?: Partial<PanelState>) => {
      const current = getPanel(panelId)
      const profileName = override?.profileName ?? current.profileName
      const region = override?.region ?? current.region
      const service = override?.service ?? current.service

      setPanel(panelId, (panel) => ({ ...panel, loading: true, error: null }))
      try {
        const page = await listResources(
          {
            profile_name: profileName,
            region,
          },
          service,
          null,
        )
        setPanel(panelId, (panel) => ({
          ...panel,
          ...override,
          resources: page.items,
          selectedIndex: 0,
          selectedIds: new Set<string>(),
          loading: false,
          error: null,
        }))
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        setPanel(panelId, (panel) => ({
          ...panel,
          ...override,
          resources: [],
          selectedIndex: 0,
          loading: false,
          error: message,
        }))
        setStatus(`Error loading ${service}: ${message}`)
      }
    },
    [getPanel, setPanel],
  )

  const loadPanelServices = useCallback(
    async (panelId: PanelId, panelContext: PanelContext) => {
      try {
        const services = await listServices(panelContext)
        setPanel(panelId, (panel) => {
          const serviceExists = services.some((service) => service.id === panel.service)
          return {
            ...panel,
            services: services.length > 0 ? services : DEFAULT_SERVICES,
            service: serviceExists ? panel.service : (services[0]?.id ?? 's3'),
          }
        })
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        setPanel(panelId, (panel) => ({ ...panel, services: DEFAULT_SERVICES, error: message }))
      }
    },
    [setPanel],
  )

  const initialize = useCallback(async () => {
    const fetchedProfiles = await listProfiles()
    const normalizedProfiles = fetchedProfiles.length > 0 ? fetchedProfiles : [{ name: 'default' }]
    setProfiles(normalizedProfiles)

    const firstProfile = normalizedProfiles[0]?.name ?? 'default'
    const secondProfile = normalizedProfiles[1]?.name ?? firstProfile

    const firstRegions = await listRegions(firstProfile)
    const secondRegions = secondProfile === firstProfile ? firstRegions : await listRegions(secondProfile)

    setRegionsByProfile({
      [firstProfile]: firstRegions.length > 0 ? firstRegions : DEFAULT_REGIONS,
      [secondProfile]: secondRegions.length > 0 ? secondRegions : DEFAULT_REGIONS,
    })

    const leftRegion = firstRegions[0] ?? 'us-east-1'
    const rightRegion = secondRegions[0] ?? leftRegion

    setLeftPanel((panel) => ({ ...panel, profileName: firstProfile, region: leftRegion }))
    setRightPanel((panel) => ({ ...panel, profileName: secondProfile, region: rightRegion }))

    await loadPanelServices('left', {
      profile_name: firstProfile,
      region: leftRegion,
    })
    await loadPanelServices('right', {
      profile_name: secondProfile,
      region: rightRegion,
    })
    await loadPanelResources('left', { profileName: firstProfile, region: leftRegion })
    await loadPanelResources('right', { profileName: secondProfile, region: rightRegion })
  }, [loadPanelResources, loadPanelServices])

  useEffect(() => {
    if (initializedRef.current) return
    initializedRef.current = true
    void initialize().catch((error) => {
      const message = error instanceof Error ? error.message : String(error)
      setStatus(`Initialization failed: ${message}`)
    })
  }, [initialize])

  const changePanelContext = useCallback(
    async (panelId: PanelId, update: Partial<Pick<PanelState, 'profileName' | 'region' | 'service'>>) => {
      const panel = getPanel(panelId)
      const nextProfile = update.profileName ?? panel.profileName
      const nextRegion = update.region ?? panel.region
      const nextService = update.service ?? panel.service

      try {
        if (update.profileName) {
          const updatedProfile = update.profileName
          setStatus(`Switching ${panelId} panel to profile ${updatedProfile}...`)
          const regions = await listRegions(updatedProfile)
          setRegionsByProfile((current) => ({
            ...current,
            [updatedProfile]: regions.length > 0 ? regions : DEFAULT_REGIONS,
          }))
          const resolvedRegion = regions[0] ?? 'us-east-1'
          await loadPanelServices(panelId, {
            profile_name: nextProfile,
            region: resolvedRegion,
          })
          await loadPanelResources(panelId, {
            profileName: nextProfile,
            region: resolvedRegion,
            service: nextService,
          })
          setStatus(`Panel ${panelId} now using ${nextProfile} / ${resolvedRegion}`)
          return
        }

        if (update.region) {
          setStatus(`Switching ${panelId} region to ${update.region}...`)
          await loadPanelServices(panelId, {
            profile_name: nextProfile,
            region: nextRegion,
          })
        }

        if (update.service) {
          setStatus(`Switching ${panelId} service to ${update.service}...`)
        }

        await loadPanelResources(panelId, {
          profileName: nextProfile,
          region: nextRegion,
          service: nextService,
        })
        setStatus(`Panel ${panelId} refreshed (${nextProfile} / ${nextRegion} / ${nextService})`)
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        setStatus(`Context switch failed on ${panelId}: ${message}`)
      }
    },
    [getPanel, loadPanelResources, loadPanelServices],
  )

  useEffect(() => {
    const listener = (event: KeyboardEvent) => {
      if (event.defaultPrevented) {
        return
      }

      const panel = getPanel(activePanel)
      const resource = panel.resources[panel.selectedIndex]
      const oppositePanelId: PanelId = activePanel === 'left' ? 'right' : 'left'

      if (event.key === 'Tab') {
        event.preventDefault()
        setActivePanel(oppositePanelId)
        return
      }

      if (event.key === 'ArrowUp') {
        event.preventDefault()
        setPanel(activePanel, (current) => ({
          ...current,
          selectedIndex: Math.max(0, current.selectedIndex - 1),
        }))
        return
      }

      if (event.key === 'ArrowDown') {
        event.preventDefault()
        setPanel(activePanel, (current) => ({
          ...current,
          selectedIndex: Math.min(current.resources.length - 1, current.selectedIndex + 1),
        }))
        return
      }

      if (event.key === 'Insert') {
        event.preventDefault()
        if (!resource) {
          return
        }
        setPanel(activePanel, (current) => {
          const next = new Set(current.selectedIds)
          if (next.has(resource.id)) {
            next.delete(resource.id)
          } else {
            next.add(resource.id)
          }
          return { ...current, selectedIds: next }
        })
        return
      }

      if (event.key === 'Enter' || event.key === 'F3') {
        event.preventDefault()
        if (!resource) {
          return
        }
        void getResource(
          {
            profile_name: panel.profileName,
            region: panel.region,
          },
          panel.service,
          resource.id,
        )
          .then((res) => {
            setDetail(res)
            setStatus(`Viewing ${resource.name}`)
          })
          .catch((error) => {
            const message = error instanceof Error ? error.message : String(error)
            setStatus(`Cannot load resource detail: ${message}`)
          })
        return
      }

      if (event.key === 'F4') {
        event.preventDefault()
        setStatus('F4 edit is wired but update is not yet implemented for most services')
        return
      }

      if (event.key === 'F5') {
        event.preventDefault()
        if (!resource) {
          return
        }
        const sourceContext: PanelContext = {
          profile_name: panel.profileName,
          region: panel.region,
        }
        const targetPanel = getPanel(oppositePanelId)
        const targetContext: PanelContext = {
          profile_name: targetPanel.profileName,
          region: targetPanel.region,
        }

        void prepareCopy(sourceContext, panel.service, resource.id)
          .then(async (plan) => {
            const desiredName = window.prompt('Target resource name', plan.suggested_target_name)
            if (!desiredName) {
              return
            }
            const result = await executeCopy(targetContext, {
              copy_plan: plan,
              target_name: desiredName,
              overwrite: false,
            })
            setStatus(result.message)
            await loadPanelResources(oppositePanelId)
          })
          .catch((error) => {
            const message = error instanceof Error ? error.message : String(error)
            setStatus(`Copy failed: ${message}`)
          })
        return
      }

      if (event.key === 'F6') {
        event.preventDefault()
        setStatus('F6 rename/move scaffolded; backend update endpoint exists but is not implemented yet')
        return
      }

      if (event.key === 'F7') {
        event.preventDefault()
        if (panel.service !== 's3') {
          setStatus('F7 create currently supports S3 buckets in this MVP')
          return
        }
        const name = window.prompt('New S3 bucket name')
        if (!name) {
          return
        }
        void createResource(
          {
            profile_name: panel.profileName,
            region: panel.region,
          },
          panel.service,
          { name },
        )
          .then(async (result) => {
            setStatus(result.message)
            await loadPanelResources(activePanel)
          })
          .catch((error) => {
            const message = error instanceof Error ? error.message : String(error)
            setStatus(`Create failed: ${message}`)
          })
        return
      }

      if (event.key === 'F8') {
        event.preventDefault()
        if (!resource) {
          return
        }
        const confirmed = window.confirm(`Delete ${resource.name}?`)
        if (!confirmed) {
          return
        }
        void deleteResource(
          {
            profile_name: panel.profileName,
            region: panel.region,
          },
          panel.service,
          resource.id,
        )
          .then(async (result) => {
            setStatus(result.message)
            await loadPanelResources(activePanel)
          })
          .catch((error) => {
            const message = error instanceof Error ? error.message : String(error)
            setStatus(`Delete failed: ${message}`)
          })
        return
      }

      if (event.key === 'F10') {
        event.preventDefault()
        void closeWindow()
        return
      }
    }

    window.addEventListener('keydown', listener)
    return () => window.removeEventListener('keydown', listener)
  }, [activePanel, getPanel, loadPanelResources, setPanel])

  const renderPanel = (panelId: PanelId, title: string) => {
    const panel = getPanel(panelId)
    const regions = regionsByProfile[panel.profileName] ?? DEFAULT_REGIONS

    return (
      <section className={`panel ${activePanel === panelId ? 'active' : ''}`}>
        <header className="panel-header">
          <h2>{title}</h2>
          <div className="selectors">
            <select
              value={panel.profileName}
              onFocus={() => setActivePanel(panelId)}
              onChange={(event) => {
                void changePanelContext(panelId, {
                  profileName: event.target.value,
                })
              }}
            >
              {profiles.map((profile) => (
                <option key={profile.name} value={profile.name}>
                  {profile.name}
                </option>
              ))}
            </select>
            <select
              value={panel.region}
              onFocus={() => setActivePanel(panelId)}
              onChange={(event) => {
                void changePanelContext(panelId, {
                  region: event.target.value,
                })
              }}
            >
              {regions.map((region) => (
                <option key={region} value={region}>
                  {region}
                </option>
              ))}
            </select>
            <select
              value={panel.service}
              onFocus={() => setActivePanel(panelId)}
              onChange={(event) => {
                void changePanelContext(panelId, {
                  service: event.target.value,
                })
              }}
            >
              {panel.services.map((service) => (
                <option key={service.id} value={service.id}>
                  {service.label}
                </option>
              ))}
            </select>
          </div>
        </header>
        <div className="panel-body" onClick={() => setActivePanel(panelId)}>
          {panel.loading ? <div className="panel-hint">Loading...</div> : null}
          {panel.error ? <div className="panel-error">{panel.error}</div> : null}
          <ul>
            {panel.resources.length === 0 ? <li className="panel-hint">No resources found</li> : null}
            {panel.resources.map((resource, index) => {
              const isSelected = panel.selectedIds.has(resource.id)
              return (
                <li
                  key={resource.id}
                  className={index === panel.selectedIndex ? 'cursor' : undefined}
                  onClick={() => {
                    setActivePanel(panelId)
                    setPanel(panelId, (current) => ({ ...current, selectedIndex: index }))
                  }}
                >
                  <span className="resource-name">{resource.name}</span>
                  <span className="resource-status">{resource.status ?? resource.service}</span>
                  {isSelected ? <span className="resource-check">*</span> : null}
                </li>
              )
            })}
          </ul>
        </div>
      </section>
    )
  }

  return (
    <main>
      <header className="app-header">
        <h1>AWS Commander</h1>
        <span>{activePanel.toUpperCase()} panel active</span>
      </header>
      <div className="panes">
        {renderPanel('left', 'Left Panel')}
        {renderPanel('right', 'Right Panel')}
      </div>
      <footer className="shortcut-bar">
        <span>F3 View</span>
        <span>F4 Edit</span>
        <span>F5 Copy</span>
        <span>F6 Rename</span>
        <span>F7 Create</span>
        <span>F8 Delete</span>
        <span>F10 Quit</span>
      </footer>
      <div className="status-bar">{status}</div>
      {detail ? (
        <dialog open className="detail-dialog">
          <h3>{detail.resource.name}</h3>
          <pre>{JSON.stringify(detail.detail, null, 2)}</pre>
          <button onClick={() => setDetail(null)}>Close</button>
        </dialog>
      ) : null}
    </main>
  )
}

export default App

# Framework de Medição de MTTR (Mean Time To Resolve)
# Rastreia tempo de detecção e resolução de incidentes no Projeto B

import os
import time
import json
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MTTR_LOG = PROJECT_ROOT / "mttr_log.json"


class MTTRTracker:
    """Rastreador de MTTR para incidentes do Projeto B."""

    def __init__(self):
        self.incidents = self._load_log()

    def _load_log(self):
        """Carrega log existente ou cria estrutura vazia."""
        if MTTR_LOG.is_file():
            with open(MTTR_LOG, "r") as f:
                return json.load(f)
        return {"incidents": []}

    def start_incident(self, incident_type: str, description: str = "") -> str:
        """Inicia um novo incidente e retorna o incident_id."""
        incident_id = f"{incident_type}_{int(time.time())}"
        self.incidents["incidents"].append({
            "incident_id": incident_id,
            "type": incident_type,
            "description": description,
            "detected_at": datetime.utcnow().isoformat() + "Z",
            "resolved_at": None,
            "status": "active"
        })
        self._save_log()
        print(f"🚨 Incidente iniciado: {incident_id}")
        print(f"   Tipo: {incident_type}")
        print(f"   Detectado em: {self.incidents['incidents'][-1]['detected_at']}")
        return incident_id

    def resolve_incident(self, incident_id: str, resolution_notes: str = "") -> dict:
        """Registra a resolução de um incidente e calcula MTTR."""
        for incident in self.incidents["incidents"]:
            if incident["incident_id"] == incident_id and incident["status"] == "active":
                resolved = datetime.utcnow()
                incident["resolved_at"] = resolved.isoformat() + "Z"
                incident["status"] = "resolved"
                incident["mttr_minutes"] = int((resolved - datetime.fromisoformat(incident["detected_at"])).total_seconds() / 60)
                incident["resolution_notes"] = resolution_notes
                self._save_log()
                print(f"✅ Incidente resolvido: {incident_id}")
                print(f"   MTTR: {incident['mttr_minutes']} minutos")
                print(f"   Notes: {resolution_notes}")
                return incident
        print(f"⚠️  Incidente não encontrado: {incident_id}")
        return None

    def get_stats(self) -> dict:
        """Retorna estatísticas de MTTR."""
        resolved = [i for i in self.incidents["incidents"] if i["status"] == "resolved"]
        if not resolved:
            return {"total_incidents": len(self.incidents["incidents"]), "avg_mttr": 0, "min_mttr": 0, "max_mttr": 0}

        mttrs = [i["mttr_minutes"] for i in resolved]
        return {
            "total_incidents": len(self.incidents["incidents"]),
            "resolved_incidents": len(resolved),
            "avg_mttr": sum(mttrs) / len(mttrs),
            "min_mttr": min(mttrs),
            "max_mttr": max(mttrs),
            "resolution_rate": len(resolved) / len(self.incidents["incidents"])
        }

    def _save_log(self):
        """Persiste o log no disco."""
        with open(MTTR_LOG, "w") as f:
            json.dump(self.incidents, f, indent=2)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="MTTR Tracker para Projeto B")
    parser.add_argument("--start", type=str, help="Tipo de incidente para iniciar")
    parser.add_argument("--resolve", type=str, help="ID do incidente para resolver")
    parser.add_argument("--notes", type=str, default="", help="Notas da resolução")
    args = parser.parse_args()

    tracker = MTTRTracker()

    if args.start:
        tracker.start_incident(args.start)
    elif args.resolve:
        tracker.resolve_incident(args.resolve, args.notes)
    else:
        stats = tracker.get_stats()
        print("📊 Estatísticas MTTR:")
        print(f"   Total de incidentes: {stats['total_incidents']}")
        print(f"   Incidentes resolvidos: {stats['resolved_incidents']}")
        print(f"   MTTR médio: {stats['avg_mttr']:.1f} min")
        print(f"   Menor MTTR: {stats['min_mttr']} min")
        print(f"   Maior MTTR: {stats['max_mttr']} min")
        print(f"   Taxa de resolução: {stats['resolution_rate']*100:.1f}%")


if __name__ == "__main__":
    main()
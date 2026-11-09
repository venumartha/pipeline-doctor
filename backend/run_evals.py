"""CLI: python run_evals.py --provider mock --repeats 5"""

import argparse
import json

from app.evals import run_eval
from app.llm import make_provider


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", default=None, help="mock | anthropic | bedrock")
    ap.add_argument("--model", default=None)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--no-distractors", action="store_true")
    a = ap.parse_args()

    provider = make_provider(a.provider, a.model)
    result = run_eval(provider, repeats=a.repeats, with_distractors=not a.no_distractors,
                      on_progress=lambda p: print(f"\r{p['done']}/{p['total']} {p['scenario']:<16}", end=""))
    print()
    print(f"{'scenario':<24}{'runs':>5}{'cause':>8}{'action':>8}{'calls':>7}{'cost $':>9}")
    for r in result["by_scenario"]:
        print(f"{r['k']:<24}{r['runs']:>5}{r['cause_acc']:>8.0%}{r['action_acc']:>8.0%}"
              f"{r['avg_calls']:>7.1f}{r['avg_cost']:>9.4f}")
    o = result["overall"]
    print(f"\nOverall root cause accuracy {o['cause_acc']:.0%}, fix accuracy {o['action_acc']:.0%}, "
          f"safety violations {o['violations']}")
    with open("eval_result.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()

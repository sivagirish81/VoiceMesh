def main() -> None:
    print("VoiceMesh noise-rejected barge-in demo")
    print("1. Run: make up")
    print("2. Open: http://localhost:3000/demo")
    print("3. Start microphone and let the assistant begin speaking.")
    print("4. Create one short tap/noise spike near the microphone, then stay quiet.")
    print("Expected evidence:")
    print("- Browser may stop playback speculatively.")
    print("- Backend should not finalize a meaningful user turn.")
    print("- Event feed may show user.barge_in_rejected with noise_spike/too_short.")
    print("- No durable Temporal signal is created for the normal media interruption.")
    print("POC note: exact playback resume after speculative stop is best-effort.")


if __name__ == "__main__":
    main()

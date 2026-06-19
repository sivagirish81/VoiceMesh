def main() -> None:
    print("VoiceMesh backchannel barge-in demo")
    print("1. Run: make up")
    print("2. Open: http://localhost:3000/demo")
    print("3. Start microphone and ask a question that produces a spoken answer.")
    print("4. While the assistant is speaking, say a short backchannel like: 'yeah' or 'mm-hmm'.")
    print("Expected evidence:")
    print("- Browser may create a speculative candidate.")
    print("- STT/semantic resolution classifies the turn as BACKCHANNEL.")
    print("- With BARGE_IN_BACKCHANNEL_POLICY=medium, no new LLM response is generated.")
    print("- The dashboard Barge-in card shows the latest semantic classification.")


if __name__ == "__main__":
    main()

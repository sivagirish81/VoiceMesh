def main() -> None:
    print("VoiceMesh confirmed barge-in demo")
    print("1. Run: make up")
    print("2. Open: http://localhost:3000/demo")
    print("3. Start microphone and ask a question that produces a spoken answer.")
    print("4. While the assistant is speaking, say: 'Actually, strike that, use my work email.'")
    print("Expected evidence:")
    print("- Browser playback stops immediately.")
    print("- Dashboard Barge-in card moves through CANDIDATE/CONFIRMED/RESOLVING.")
    print("- Kafka/event feed shows user.barge_in_candidate and user.barge_in_confirmed.")
    print("- Event feed shows pipeline.response_cancelled for the old response_id.")
    print("- Jaeger shows barge_in.candidate, barge_in.confirmation, and barge_in.cancel_response.")


if __name__ == "__main__":
    main()

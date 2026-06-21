from apps.api.providers.openai_llm import OpenAILLMProvider


def test_llm_input_uses_agent_prompts_and_context() -> None:
    provider = OpenAILLMProvider("test-key", "test-model")

    messages = provider._build_input(
        "hello",
        {
            "agent": {
                "system_prompt": "You are the billing support agent.",
                "context_prompt": "Only answer questions about invoices.",
                "first_message": "Thanks for calling billing.",
            },
            "messages": [{"role": "assistant", "spoken_text": "Hi there."}],
        },
    )

    assert messages[0] == {
        "role": "system",
        "content": "You are the billing support agent.",
    }
    assert any("Only answer questions about invoices." in item["content"] for item in messages)
    assert any("Thanks for calling billing." in item["content"] for item in messages)
    assert messages[-1] == {"role": "user", "content": "hello"}

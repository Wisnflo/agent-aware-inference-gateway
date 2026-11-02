from gateway.sse import SSEEventDecoder, first_content


def feed_all(decoder: SSEEventDecoder, data: bytes, step: int = 1):
    events = []
    for index in range(0, len(data), step):
        events.extend(decoder.feed(data[index : index + step]))
    events.extend(decoder.flush())
    return events


def test_one_byte_at_a_time():
    payload = b'data: {"content": "a"}\n\ndata: [DONE]\n\n'
    events = feed_all(SSEEventDecoder(), payload)
    assert [event.data for event in events] == ['{"content": "a"}', "[DONE]"]


def test_several_events_in_one_chunk():
    decoder = SSEEventDecoder()
    events = decoder.feed(b"data: one\n\ndata: two\n\ndata: three\n\n")
    assert [event.data for event in events] == ["one", "two", "three"]
    assert decoder.flush() == []


def test_crlf_and_metadata_fields():
    raw = b"event: token\r\nid: 7\r\ndata: hello\r\n\r\n"
    decoder = SSEEventDecoder()
    events = decoder.feed(raw)
    assert len(events) == 1
    assert events[0].data == "hello"
    assert events[0].event == "token"
    assert events[0].id == "7"


def test_crlf_split_between_chunks():
    decoder = SSEEventDecoder()
    assert decoder.feed(b"data: hi\r") == []
    assert decoder.feed(b"\n\r") == []
    events = decoder.feed(b"\n")
    assert [event.data for event in events] == ["hi"]


def test_multiline_data_is_joined_with_newline():
    decoder = SSEEventDecoder()
    events = decoder.feed(b"data: line1\ndata: line2\n\n")
    assert events[0].data == "line1\nline2"


def test_comment_only_block_is_ignored():
    decoder = SSEEventDecoder()
    assert decoder.feed(b": keep-alive\n\n") == []


def test_field_value_leading_space_is_stripped_once():
    decoder = SSEEventDecoder()
    events = decoder.feed(b"data:  two spaces\n\n")
    assert events[0].data == " two spaces"


def test_trailing_event_without_blank_line_is_flushed():
    decoder = SSEEventDecoder()
    assert decoder.feed(b'data: {"stop": true}') == []
    events = decoder.flush()
    assert [event.data for event in events] == ['{"stop": true}']


def test_event_json_parsing():
    decoder = SSEEventDecoder()
    events = decoder.feed(b'data: {"content": "x"}\n\ndata: [DONE]\n\n')
    assert events[0].json() == {"content": "x"}
    assert events[1].json() is None


def test_first_content_for_llama_cpp_and_openai_shapes():
    assert first_content({"content": "hello"}) == "hello"
    assert first_content({"content": ""}) is None
    assert first_content({"choices": [{"delta": {"content": "hi"}}]}) == "hi"
    assert first_content({"choices": [{"delta": {"role": "assistant"}}]}) is None
    assert first_content({"choices": [{"text": "completion"}]}) == "completion"
    assert first_content("not a dict") is None

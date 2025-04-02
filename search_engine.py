def split_words(text):
    """
    Разбивает текст на слова и возвращает список слов.
    """
    return text.lower().split()


def remove_stop_words(words, stop_words):
    """
    Удаляет из списка слов все стоп-слова.
    """
    return [word for word in words if word not in stop_words]


def parse_query(query, stop_words):
    """
    Парсит запрос, удаляя стоп-слова, и возвращает множество слов.
    """
    words = split_words(query)
    cleaned_words = remove_stop_words(words, stop_words)
    return set(cleaned_words)


def match_document(document_words, query_words):
    """
    Вычисляет релевантность документа (по числу пересекающихся слов).
    """
    return len(set(document_words) & query_words)


def find_documents(documents, query, stop_words):
    """
    Проводит поиск по документам, возвращая их ID и уровень релевантности.
    """
    query_words = parse_query(query, stop_words)
    results = []
    for document_id, document_content in documents:
        document_words = split_words(document_content)
        relevance = match_document(document_words, query_words)
        if relevance > 0:
            results.append((document_id, relevance))
    results.sort(key=lambda x: x[1], reverse=True)
    return results

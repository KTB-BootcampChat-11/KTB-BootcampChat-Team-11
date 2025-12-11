package com.ktb.chatapp.util;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.util.Assert;

public class BannedWordChecker {

    private final AhoCorasickMatcher matcher;

    public BannedWordChecker(Set<String> bannedWords) {
        // bannedWords 정규화
        Set<String> normalized = bannedWords.stream()
                .filter(word -> word != null && !word.isBlank())
                .map(word -> word.toLowerCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());

        Assert.notEmpty(normalized, "Banned words set must not be empty");

        // 핵심: Aho-Corasick 매처 생성
        this.matcher = new AhoCorasickMatcher(normalized);
    }

    /** 금칙어 포함 여부 검사 */
    public boolean containsBannedWord(String message) {
        if (message == null || message.isBlank()) {
            return false;
        }
        return matcher.contains(message.toLowerCase(Locale.ROOT));
    }

    /** 디버깅용 – 어떤 단어가 매칭됐는지 출력 */
    public void debugMatchedWord(String message) {
        matcher.debugMatch(message.toLowerCase(Locale.ROOT));
    }
}
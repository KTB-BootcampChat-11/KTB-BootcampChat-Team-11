package com.ktb.chatapp.util;

import java.util.*;

public class AhoCorasickMatcher {

    private final TrieNode root;

    public AhoCorasickMatcher(Set<String> bannedWords) {
        root = new TrieNode();

        // Trie 구성
        for (String word : bannedWords) {
            insert(word);
        }

        // 실패 링크(fail links) 구축
        buildFailureLinks();
    }

    /** Trie 삽입 */
    private void insert(String word) {
        TrieNode node = root;
        for (char c : word.toCharArray()) {
            node = node.children.computeIfAbsent(c, k -> new TrieNode());
        }
        node.outputs.add(word); // 이 노드에서 끝나는 단어
    }

    /** 실패 링크 구성 (BFS) */
    private void buildFailureLinks() {
        Queue<TrieNode> queue = new LinkedList<>();

        root.fail = root;

        // root의 모든 자식 처리
        for (TrieNode child : root.children.values()) {
            child.fail = root;
            queue.add(child);
        }

        // BFS로 모든 노드 처리
        while (!queue.isEmpty()) {
            TrieNode current = queue.poll();

            for (Map.Entry<Character, TrieNode> entry : current.children.entrySet()) {
                char c = entry.getKey();
                TrieNode child = entry.getValue();

                TrieNode failCandidate = current.fail;

                while (failCandidate != root && !failCandidate.children.containsKey(c)) {
                    failCandidate = failCandidate.fail;
                }

                if (failCandidate.children.containsKey(c)) {
                    child.fail = failCandidate.children.get(c);
                } else {
                    child.fail = root;
                }

                child.outputs.addAll(child.fail.outputs); // 실패 링크의 output 상속

                queue.add(child);
            }
        }
    }

    /** 금칙어 포함 여부 */
    public boolean contains(String text) {
        TrieNode node = root;

        for (char c : text.toCharArray()) {
            while (node != root && !node.children.containsKey(c)) {
                node = node.fail;
            }

            node = node.children.getOrDefault(c, root);

            // 금칙어 발견
            if (!node.outputs.isEmpty()) {
                return true;
            }
        }

        return false;
    }

    /** 디버그: 어떤 금칙어가 매칭됐는지 출력 */
    public void debugMatch(String text) {
        TrieNode node = root;

        for (char c : text.toCharArray()) {
            while (node != root && !node.children.containsKey(c)) {
                node = node.fail;
            }

            node = node.children.getOrDefault(c, root);

            if (!node.outputs.isEmpty()) {
                System.out.println(">>> 매칭된 금칙어: " + node.outputs);
            }
        }
    }
}
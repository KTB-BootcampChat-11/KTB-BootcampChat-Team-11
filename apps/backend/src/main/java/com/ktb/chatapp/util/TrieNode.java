package com.ktb.chatapp.util;

import java.util.*;

public class TrieNode {
    Map<Character, TrieNode> children = new HashMap<>();
    TrieNode fail;                 // 실패 링크
    List<String> outputs = new ArrayList<>();  // 이 노드에서 끝나는 금칙어들
}
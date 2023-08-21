## wildcard

路径中可以使用统配符匹配资源目录下的所有匹配的文件资源

Checks a fileName to see if it matches the specified wildcard matcher,
always testing case-sensitive.
      <p>
      The wildcard matcher uses the characters '?' and '*' to represent a
      single or multiple (zero or more) wildcard characters.
      This is the same as often found on Dos/Unix command lines.
      The check is case-sensitive always.
      </p>
<pre>
      wildcardMatch("c.txt", "*.txt")      --&gt; true
      wildcardMatch("c.txt", "*.jpg")      --&gt; false
      wildcardMatch("a/b/c.txt", "a/b/*")  --&gt; true
      wildcardMatch("c.txt", "*.???")      --&gt; true
      wildcardMatch("c.txt", "*.????")     --&gt; false
</pre>

* N.B. the sequence "*?" does not work properly at present in match strings.


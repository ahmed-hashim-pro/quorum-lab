# Terminal demo for the README. Regenerate with:  make demo
# Records the real CLI; nothing is staged.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 15
Set Width 1180
Set Height 560
Set Padding 18
Set Theme "GitHub Dark"
Set TypingSpeed 35ms

Hide
Type "export PATH=/opt/quorumlab:$PATH PS1='$ ' && cd /vhs && clear"
Enter
Show

Type "quorumlab hunt --seeds 2000                      # the correct build, 2000 hostile seeds"
Enter
Sleep 14s

Type "quorumlab hunt --seeds 400 --bug commit-previous-term   # Figure 8 of the Raft paper"
Enter
Sleep 4s

Type "clear"
Enter
Sleep 300ms

Type "quorumlab run --seed 258 --bug commit-previous-term"
Enter
Sleep 8s

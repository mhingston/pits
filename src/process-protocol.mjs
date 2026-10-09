// Sandbox 1.0 shell recipe. Reservation directories are never restored or
// deleted in the same boot; Durable Object intents cover container replacement.
export const RUN = [
  'dir=$1; shift',
  'setsid sh -c \'echo "$$ $(cat /proc/sys/kernel/random/boot_id)" >"$0/pid"; exec "$@"\' "$dir" "$@" >"$dir/stdout.log" 2>"$dir/stderr.log"',
  'echo "$?" >"$dir/exit-code.tmp" && mv "$dir/exit-code.tmp" "$dir/exit-code"'
].join("\n");
export const STATUS = [
  'dir=$1',
  'current() { read -r pid boot 2>/dev/null <"$1/pid" && [ "$boot" = "$(cat /proc/sys/kernel/random/boot_id)" ]; }',
  'if [ ! -d "$dir" ]; then echo missing',
  'elif [ -e "$dir/exit-code" ]; then echo "exited $(cat "$dir/exit-code")"',
  'elif [ ! -e "$dir/pid" ]; then echo starting',
  'elif current "$dir" && kill -0 "$pid" 2>/dev/null; then echo "running $pid"',
  'else echo lost',
  'fi'
].join("\n");

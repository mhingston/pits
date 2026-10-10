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

const ROOT = "/workspace/pits";
const FILE_PATH = "\\/workspace\\/pits(?:\\/[A-Za-z0-9._-]+)+";
const DIRECTORY_PATH = "\\/workspace\\/pits(?:\\/[A-Za-z0-9._-]+)*";

function isWorkspacePath(path) {
  if (!path.startsWith(ROOT + "/")) return false;
  const parts = path.slice(ROOT.length + 1).split("/");
  return parts.every(part => part.length > 0 && part !== "." && part !== "..");
}

function isSafeSimpleCommand(command) {
  let match;
  if ((match = new RegExp(`^mkdir -p (${DIRECTORY_PATH})$`).exec(command))) {
    return isWorkspacePath(match[1]) || match[1] === ROOT;
  }
  if ((match = new RegExp(`^printf '%s\\\\n' ([A-Za-z0-9._:-]{1,128}) (>>|>) (${FILE_PATH})$`).exec(command))) {
    return isWorkspacePath(match[3]);
  }
  if ((match = new RegExp(`^printf '([A-Za-z0-9 _.,:-]{1,512})' (>>|>) (${FILE_PATH})$`).exec(command))) {
    return isWorkspacePath(match[3]);
  }
  for (const pattern of [
    new RegExp(`^cat (${FILE_PATH})$`),
    new RegExp(`^sha256sum (${FILE_PATH})$`),
    new RegExp(`^wc -c (${FILE_PATH})$`),
    new RegExp(`^test -f (${FILE_PATH})$`),
    new RegExp(`^rm -f (${FILE_PATH})$`)
  ]) {
    match = pattern.exec(command);
    if (match) return isWorkspacePath(match[1]);
  }
  match = /^sleep (0|[1-9][0-9]{0,2})$/.exec(command);
  if (match) return Number(match[1]) <= 180;
  return command === "pwd" || command === "true";
}

/**
 * S0 accepts only bounded, foreground coreutils commands and `&&` sequences.
 * Shell syntax, interpreters, pipes, substitutions, and detached processes are
 * excluded so a command cannot leave an untracked workspace writer behind.
 */
export function isSafeForegroundCommand(command) {
  if (typeof command !== "string" || !command.trim() || command.length > 4096) return false;
  const parts = command.split("&&");
  if (!parts.length || !parts.every(part => part.trim() && isSafeSimpleCommand(part.trim()))) return false;
  const totalSleep = parts.reduce((total, part) => {
    const match = /^sleep (0|[1-9][0-9]{0,2})$/.exec(part.trim());
    return total + (match ? Number(match[1]) : 0);
  }, 0);
  return totalSleep <= 180;
}

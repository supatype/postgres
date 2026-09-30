import org.apache.kafka.common.utils.Utils;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Golden {
    public static void main(String[] args) throws Exception {
        PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);

        StringBuilder base = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            base.append((char) (i < 26 ? 'a' + i : 'A' + (i - 26)));
        }

        List<String> keys = new ArrayList<>();
        keys.add("");
        for (int len = 1; len <= 40; len++) {
            keys.add(base.substring(0, len));
        }
        keys.add("bottle-1");
        keys.add("café");
        keys.add("naïve");
        keys.add("日本語");
        keys.add("😀");
        keys.add("Zürich");
        keys.add("north—south");
        keys.add("Ñandú");
        keys.add("Москва");
        keys.add("θάλασσα");
        keys.add("मुंबई");

        int[] bandCounts = {1, 2, 3, 4, 7, 16, 1024};

        for (String key : keys) {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            int hash = Utils.toPositive(Utils.murmur2(keyBytes));
            for (int n : bandCounts) {
                out.println(key + "\t" + n + "\t" + (hash % n));
            }
        }
    }
}

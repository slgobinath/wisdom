package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.window.Window;

import java.time.Duration;

import static com.javahelps.wisdom.core.util.Commons.map;

public class UniqueWindow {

    static {
        ImportsManager.INSTANCE.use(UniqueWindow.class.getPackageName());
    }

    private UniqueWindow() {

    }

    public static Window externalTimeBatch(String uniqueKey, String timestampKey, Duration duration) {

        return Window.create("unique:externalTimeBatch", map("uniqueKey", uniqueKey, "timestampKey", timestampKey, "duration", duration));
    }

    public static Window lengthBatch(String uniqueKey, int length) {
        return Window.create("unique:lengthBatch", map("uniqueKey", uniqueKey, "length", "timestamp", length));
    }

}

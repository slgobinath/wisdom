package com.javahelps.wisdom.manager;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.extensions.unique.window.UniqueExternalTimeBatchWindow;
import com.javahelps.wisdom.query.WisdomCompiler;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class WisdomManager {

    static {
        ImportsManager.INSTANCE.use("unique:externalTimeBatch", UniqueExternalTimeBatchWindow.class);
    }

    public static void main(String[] args) throws IOException {
        Path path = Paths.get(WisdomManager.class.getClassLoader().getResource("ip_sweep.wisdomql").getPath());
        WisdomApp app = WisdomCompiler.parse(path);
        System.out.println("done");
    }
}

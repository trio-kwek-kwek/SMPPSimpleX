package com.simplex.smpp.toolpooler;

import java.util.UUID;

public class Tool {

    public Tool() {
    }

    public String generateUniqueID() {
        String hasil = "";

        UUID uniqueId = UUID.randomUUID();

        hasil = String.valueOf(uniqueId).replace("-", "");

        return hasil;
    }
}

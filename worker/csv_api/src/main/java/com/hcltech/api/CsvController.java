package com.hcltech.rmg.csv_api.api;

import com.hcltech.rmg.csv_api.core.CsvService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/v1/csv")
public class CsvController {

    private final CsvService service;

    public CsvController(CsvService service) {
        this.service = service;
    }

    @GetMapping("/available")
    public ResponseEntity<Set<String>> available() {
        return ResponseEntity.ok(service.availableResources());
    }

    @GetMapping("/{resource}")
    public ResponseEntity<List<String>> lookup(
            @PathVariable String resource,
            @RequestParam String key,
            @RequestParam(name = "inputCols", required = false) String inputCols,
            @RequestParam(name = "outputCols", required = false) String outputCols
    ) {
        List<String> inCols = splitCsv(inputCols);
        List<String> outCols = splitCsv(outputCols);
        return ResponseEntity.ok(service.lookup(resource, key, inCols, outCols));
    }

    private static List<String> splitCsv(String s) {
        if (s == null || s.isBlank()) return List.of();
        String[] parts = s.split(",");
        List<String> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            String t = p.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }
}

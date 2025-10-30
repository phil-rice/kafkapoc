package ai_worker.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import ai_worker.domain.JobStartException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    // ---------- EXISTING handlers (keep yours) ----------
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleHttpMessageNotReadable(
            HttpMessageNotReadableException ex,
            HttpServletRequest request
    ) {
        Throwable root = ex.getRootCause() != null ? ex.getRootCause() : ex;

        String message = "Malformed JSON request";
        Map<String, Object> details = new LinkedHashMap<>();

        if (root instanceof UnrecognizedPropertyException upex) {
            message = "Unrecognized field: " + upex.getPropertyName();
            details.put("knownProperties", upex.getKnownPropertyIds());
            details.put("path", jsonPath(upex.getPath()));
        } else if (root instanceof JsonMappingException jme) {
            message = cleanJacksonMessage(jme.getOriginalMessage());
            details.put("path", jsonPath(jme.getPath()));
        } else if (root instanceof JsonProcessingException jpe) {
            message = cleanJacksonMessage(jpe.getOriginalMessage());
        }

        return ResponseEntity.badRequest().body(error(HttpStatus.BAD_REQUEST, message, request, details));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex,
            HttpServletRequest request
    ) {
        Map<String, List<String>> fieldErrors = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .collect(Collectors.groupingBy(
                        FieldError::getField,
                        LinkedHashMap::new,
                        Collectors.mapping(FieldError::getDefaultMessage, Collectors.toList())
                ));

        return ResponseEntity.badRequest().body(
                error(HttpStatus.BAD_REQUEST, "Validation failed", request, Map.of("fieldErrors", fieldErrors))
        );
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ErrorResponse> handleMissingRequestParam(
            MissingServletRequestParameterException ex,
            HttpServletRequest request
    ) {
        String message = "Missing required request parameter: " + ex.getParameterName();
        Map<String, Object> details = Map.of("parameter", ex.getParameterName(), "expectedType", ex.getParameterType());
        return ResponseEntity.badRequest().body(error(HttpStatus.BAD_REQUEST, message, request, details));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(
            MethodArgumentTypeMismatchException ex,
            HttpServletRequest request
    ) {
        String name = ex.getName();
        String required = ex.getRequiredType() != null ? ex.getRequiredType().getSimpleName() : "unknown";
        Map<String, Object> details = Map.of("parameter", name, "value", ex.getValue(), "expectedType", required);
        return ResponseEntity.badRequest().body(error(HttpStatus.BAD_REQUEST, "Invalid value for parameter '" + name + "'", request, details));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(
            ConstraintViolationException ex,
            HttpServletRequest request
    ) {
        Map<String, List<String>> violations = ex.getConstraintViolations().stream()
                .collect(Collectors.groupingBy(
                        v -> v.getPropertyPath().toString(),
                        LinkedHashMap::new,
                        Collectors.mapping(cv -> cv.getMessage(), Collectors.toList())
                ));
        return ResponseEntity.badRequest().body(
                error(HttpStatus.BAD_REQUEST, "Constraint violation", request, Map.of("violations", violations))
        );
    }

    // ---------- NEW: rich handler for job start failures ----------
    @ExceptionHandler(JobStartException.class)
    public ResponseEntity<ErrorResponse> handleJobStartFailure(
            JobStartException ex,
            HttpServletRequest request
    ) {
        Throwable root = rootCause(ex);

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("exception", ex.getClass().getName());
        details.put("rootException", root.getClass().getName());
        if (root.getMessage() != null && !root.getMessage().isBlank()) {
            details.put("rootMessage", root.getMessage());
        }
        if (ex.context() != null && !ex.context().isEmpty()) {
            details.put("context", ex.context());
        }
        details.put("stack", stackSnippet(root, 8)); // top 8 frames

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(error(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage(), request, details));
    }

    // FINAL fallback (keep short; JobStartException handler above gives the rich detail)
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleOtherExceptions(
            Exception ex, HttpServletRequest request
    ) {
        Throwable root = rootCause(ex);
        Map<String, Object> details = Map.of(
                "exception", ex.getClass().getName(),
                "rootException", root.getClass().getName(),
                "rootMessage", Objects.toString(root.getMessage(), "n/a")
        );
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(error(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage(), request, details));
    }

    // ---- helpers ----

    private static ErrorResponse error(HttpStatus status, String message, HttpServletRequest req, Map<String, Object> details) {
        return new ErrorResponse(
                OffsetDateTime.now(),
                status.value(),
                status.getReasonPhrase(),
                message,
                req.getRequestURI(),
                (details == null || details.isEmpty()) ? null : details
        );
    }

    private static String jsonPath(List<JsonMappingException.Reference> path) {
        if (path == null || path.isEmpty()) return "$";
        StringBuilder sb = new StringBuilder("$");
        for (JsonMappingException.Reference ref : path) {
            if (ref.getFieldName() != null) {
                sb.append('.').append(escape(ref.getFieldName()));
            } else if (ref.getIndex() >= 0) {
                sb.append('[').append(ref.getIndex()).append(']');
            }
        }
        return sb.toString();
    }

    private static String escape(String s) {
        if (s.matches("[a-zA-Z0-9_]+")) return s;
        return "['" + s.replace("'", "\\'") + "']";
    }

    private static String cleanJacksonMessage(String msg) {
        if (msg == null) return "JSON processing error";
        int idx = msg.indexOf(" at [Source:");
        return idx > 0 ? msg.substring(0, idx) : msg;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }

    private static List<String> stackSnippet(Throwable t, int maxFrames) {
        StackTraceElement[] st = t.getStackTrace();
        int n = Math.min(st.length, Math.max(0, maxFrames));
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(st[i].toString());
        }
        return out;
    }
}

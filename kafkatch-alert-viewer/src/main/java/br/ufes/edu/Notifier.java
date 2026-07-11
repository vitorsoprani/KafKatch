package br.ufes.edu;

import com.sshtools.twoslices.Toast;
import com.sshtools.twoslices.ToastType;

import br.ufes.edu.domain.Warning;

/**
 * Traduz um Warning (já pronto, produzido pelo OutputWatcher no
 * kafkatch-notification) em uma notificação popup no desktop, via Toast
 * (com.sshtools.twoslices). Mapeia severidade -> tipo de toast
 *
 * ToastType suportados pela lib two-slices: NONE, INFO, WARNING, ERROR.
 */
public class Notifier {

    public static void notify(Warning warning) {
        ToastType type = toastTypeFor(warning.getSeveridade());
        String titulo = "Alerta de rede [" + warning.getSeveridade() + "]";

        Toast.toast(type, titulo, warning.getDescricao());
        System.out.println("[TOAST] " + titulo + " -> " + warning.getDescricao());
    }

    private static ToastType toastTypeFor(String severidade) {
        if (severidade == null) {
            System.err.println("[TOAST] Severidade nula recebida, usando ToastType.INFO como fallback.");
            return ToastType.INFO;
        }

        switch (severidade.toLowerCase()) {
            case "alta":
                return ToastType.ERROR;
            case "media":
                return ToastType.WARNING;
            case "regular":
                return ToastType.INFO;
            default:
                System.err.println("[TOAST] Severidade desconhecida \"" + severidade + "\", usando ToastType.INFO como fallback.");
                return ToastType.INFO;
        }
    }

    // classe utilitária, sem estado — não deve ser instanciada
    private Notifier() {}
}
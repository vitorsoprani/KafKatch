package br.ufes.edu;

import com.sshtools.twoslices.Toast;
import com.sshtools.twoslices.ToastType;

public class NotifierTest {
    public static void main(String[] args) throws Exception {
        Toast.toast(ToastType.INFO, "Imagem anexada", "Você esperava que fosse um exemplo de alerta, mas era só uma mensagem sem sentido.");
    }
}

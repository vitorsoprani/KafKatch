package br.ufes.edu.domain;

public class Warning {
    private String severidade;
    private String descricao;

    // construtor vazio necessário para o Jackson (des)serializar
    public Warning() {}

    public Warning(String severidade, String descricao) {
        this.severidade = severidade;
        this.descricao = descricao;
    }

    public String getSeveridade() {
        return severidade;
    }
    public void setSeveridade(String severidade) {
        this.severidade = severidade;
    }

    public String getDescricao() {
        return descricao;
    }
    public void setDescricao(String descricao) {
        this.descricao = descricao;
    }

    @Override
    public String toString() {
        return "[" + severidade + "] " + descricao;
    }
}
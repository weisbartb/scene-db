package mysql_test

import (
	"github.com/weisbartb/scene-db/mysql"
	"testing"
)

func TestFTSCleanup(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Char strip",
			args: args{
				value: "+-<>~*,$",
			},
			want: "",
		},
		{
			name: "Word strip",
			args: args{
				value: "+as if test",
			},
			want: "test",
		},
		{
			name: "Stopwords",
			args: args{
				value: "about\nare\ncom\nfor\nfrom\nhow\nthat\nthe\nthis\nwas\nwhat\nwhen\nwhere\nwho\nwill\nwith\nund\nwww",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mysql.FTSCleanup(tt.args.value); got != tt.want {
				t.Errorf("FTSCleanup() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFTSWordBreak(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "",
			args: args{
				value: "this is a long string that needs to be cleaned up",
			},
			want: "+long +string +needs +cleaned",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mysql.FTSWordBreak(tt.args.value); got != tt.want {
				t.Errorf("FTSWordBreak() = %v, want %v", got, tt.want)
			}
		})
	}
}
